"""
Scenario: DB Slow Query

Root cause: a missing index on the orders table causes full table scans.
Effect:     order-service p99 latency spikes from ~80ms to 8-12s,
            causing api-gateway to timeout and return 504s to frontend.

What shows up in Splunk:
  - Tag Spotlight: error/latency isolated to order-service > POST /api/v1/orders
  - APM waterfall: db.query span for SELECT orders dominates wall time
  - Related Infrastructure: db-primary CPU spikes to 90%+
  - AlwaysOn Profiling (if enabled): hot path in JPA query executor
"""

import random
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

from .base import BaseScenario
from ..topology import RequestContext, SERVICES
from ..emitter import get_tracer, start_span, http_attrs, db_attrs


class DbSlowdownScenario(BaseScenario):
    name = "db_slowdown"
    description = "Missing DB index causes full table scan → order-service p99 spikes to 8-12s"
    affected_service = "order-service"
    affected_operation = "SELECT orders"

    def setup(self, topology=None):
        g = topology.get_service if topology else (lambda role: SERVICES[role])
        self._t_frontend = get_tracer(g("frontend").name,     g("frontend").language,     g("frontend").version)
        self._t_gateway  = get_tracer(g("api-gateway").name,  g("api-gateway").language,  g("api-gateway").version)
        self._t_order    = get_tracer(g("order-service").name, g("order-service").language, g("order-service").version)
        self._t_user     = get_tracer(g("user-service").name, g("user-service").language,  g("user-service").version)
        self._t_db       = get_tracer(g("postgres").name,     g("postgres").language,      g("postgres").version, host="db-primary")
        self._t_cache    = get_tracer(g("redis").name,        g("redis").language,         g("redis").version)
        ov = topology.scenario_overrides.get("db_slowdown", {}) if topology else {}
        self._root_endpoint  = ov.get("root_endpoint", "POST /api/v1/orders")
        self._slow_query     = ov.get("slow_query", "SELECT * FROM orders WHERE customer_id = $1 ORDER BY created_at DESC")
        self._slow_table     = ov.get("slow_table", "orders")

    def run_normal(self, req: RequestContext, parent_ctx: Context) -> None:
        self._emit(req, db_ms=random.randint(20, 60), gateway_timeout=False)

    def run_anomaly(self, req: RequestContext, parent_ctx: Context) -> None:
        self._emit(req, db_ms=random.randint(6000, 12000), gateway_timeout=True)

    def _emit(self, req: RequestContext, db_ms: int, gateway_timeout: bool):
        common = {
            "region": req.region,
            "customer.tier": req.customer_tier,
            "user.id": req.user_id,
        }

        with start_span(self._t_frontend, "GET /checkout", duration_ms=20,
                        attrs={**common, **http_attrs("GET", "/checkout", 200)}) as fe_span:
            ctx = None  # child spans use current context automatically

            with start_span(self._t_gateway, "POST /api/v1/orders",
                            kind=SpanKind.SERVER, duration_ms=15,
                            attrs={**common, **http_attrs("POST", "/api/v1/orders", 504 if gateway_timeout else 200)},
                            error=gateway_timeout, error_msg="upstream timeout: order-service"):

                # Validate user (fast, always succeeds)
                with start_span(self._t_user, "user.validate",
                                kind=SpanKind.SERVER, duration_ms=12,
                                attrs={**common, **http_attrs("GET", "/api/v1/users/validate", 200)}):
                    with start_span(self._t_cache, "GET user:session",
                                    kind=SpanKind.CLIENT, duration_ms=2,
                                    attrs={**common, "db.system": "redis", "db.operation": "GET", "db.redis.key": f"user:{req.user_id}"}):
                        pass

                # Place order (slow DB query is here)
                with start_span(self._t_order, "order.create",
                                kind=SpanKind.SERVER, duration_ms=db_ms + 30,
                                attrs={**common, **http_attrs("POST", "/api/v1/orders", 504 if gateway_timeout else 201)},
                                error=gateway_timeout, error_msg="db query timeout after 10s"):

                    # Check inventory (fast)
                    with start_span(self._t_order, "inventory.check",
                                    kind=SpanKind.CLIENT, duration_ms=15,
                                    attrs={**common, "rpc.system": "grpc", "rpc.service": "InventoryService", "rpc.method": "CheckStock"}):
                        pass

                    # THE SLOW QUERY — full table scan (missing index on customer_id)
                    with start_span(self._t_db, f"SELECT {self._slow_table}",
                                    kind=SpanKind.CLIENT, duration_ms=db_ms,
                                    jitter_ms=500,
                                    attrs={**common,
                                           **db_attrs("postgresql", "SELECT", self._slow_table, self._slow_query),
                                           "db.postgresql.rows": random.randint(5000, 50000),
                                           "host.name": "db-primary"}):
                        pass

                    if not gateway_timeout:
                        with start_span(self._t_db, "INSERT orders",
                                        kind=SpanKind.CLIENT, duration_ms=8,
                                        attrs={**common, **db_attrs("postgresql", "INSERT", "orders")}):
                            pass
