"""
Scenario: Cache Miss Storm (Redis Failure)

Root cause: Redis primary goes down (OOM kill). All cache reads miss → product-service
            falls back to PostgreSQL for every request. DB becomes overwhelmed.

Effect:     product-service latency goes from ~5ms (cache hit) to 200-400ms (DB).
            DB CPU hits 100%. order-service starts seeing DB timeout errors.

What shows up in Splunk:
  - Tag Spotlight: latency spike on ALL regions (global Redis cluster)
  - APM: product-service spans show redis GET error + postgres SELECT fallback
  - Related Infrastructure: redis-primary disappears from host map, db-primary CPU spikes
  - Service Map: redis edge breaks, postgres edge thickens
"""

import random
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

from .base import BaseScenario
from ..topology import RequestContext, SERVICES
from ..emitter import get_tracer, start_span, http_attrs, db_attrs


class CacheMissStormScenario(BaseScenario):
    name = "cache_miss_storm"
    description = "Redis OOM kill → all product requests fall back to DB → DB CPU 100%"
    affected_service = "redis"
    affected_operation = "GET product:*"

    def setup(self, topology=None):
        g = topology.get_service if topology else (lambda role: SERVICES[role])
        self._t_frontend = get_tracer(g("frontend").name,       g("frontend").language,       g("frontend").version)
        self._t_gateway  = get_tracer(g("api-gateway").name,    g("api-gateway").language,    g("api-gateway").version)
        self._t_product  = get_tracer(g("product-service").name, g("product-service").language, g("product-service").version)
        self._t_cache    = get_tracer(g("redis").name,          g("redis").language,          g("redis").version)
        self._t_db       = get_tracer(g("postgres").name,       g("postgres").language,       g("postgres").version)

    def run_normal(self, req: RequestContext, parent_ctx: Context) -> None:
        req.http_method = "GET"
        req.endpoint = "/api/v1/products"
        self._emit(req, cache_down=False)

    def run_anomaly(self, req: RequestContext, parent_ctx: Context) -> None:
        req.http_method = "GET"
        req.endpoint = "/api/v1/products"
        self._emit(req, cache_down=True)

    def _emit(self, req: RequestContext, cache_down: bool):
        product_id = f"prod-{random.randint(1000, 9999)}"
        common = {
            "region": req.region,
            "customer.tier": req.customer_tier,
            "product.id": product_id,
        }
        db_overloaded = cache_down and random.random() < 0.6
        db_ms = random.randint(200, 500) if db_overloaded else random.randint(15, 40)
        db_error = db_overloaded and random.random() < 0.3

        with start_span(self._t_frontend, "GET /products/:id", duration_ms=10,
                        attrs={**common, **http_attrs("GET", "/products/:id", 503 if db_error else 200)},
                        error=db_error):

            with start_span(self._t_gateway, "GET /api/v1/products/:id",
                            kind=SpanKind.SERVER, duration_ms=8,
                            attrs={**common, **http_attrs("GET", "/api/v1/products/:id", 503 if db_error else 200)},
                            error=db_error):

                with start_span(self._t_product, "product.get",
                                kind=SpanKind.SERVER, duration_ms=db_ms + 20,
                                attrs={**common, **http_attrs("GET", "/api/v1/products/:id", 503 if db_error else 200)},
                                error=db_error, error_msg="DB query timeout" if db_error else ""):

                    # Redis GET — fails when cache is down
                    with start_span(self._t_cache, f"GET product:{product_id}",
                                    kind=SpanKind.CLIENT, duration_ms=1 if not cache_down else 3000,
                                    attrs={**common,
                                           "db.system": "redis",
                                           "db.operation": "GET",
                                           "db.redis.key": f"product:{product_id}",
                                           "cache.hit": not cache_down},
                                    error=cache_down,
                                    error_msg="ECONNREFUSED 127.0.0.1:6379 - Redis is down"):
                        pass

                    # DB fallback — always happens on cache miss
                    if cache_down:
                        with start_span(self._t_db, "SELECT products",
                                        kind=SpanKind.CLIENT, duration_ms=db_ms,
                                        jitter_ms=50,
                                        attrs={**common,
                                               **db_attrs("postgresql", "SELECT", "products",
                                                          "SELECT * FROM products WHERE id = $1"),
                                               "db.postgresql.rows": 1,
                                               "host.name": "db-primary"},
                                        error=db_error,
                                        error_msg="canceling statement due to conflict with recovery"):
                            pass
