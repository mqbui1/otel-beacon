"""
Scenario: Span Spike (Traffic Surge)

Root cause: A marketing email campaign triggers a 10x surge in traffic.
            All services are hit uniformly — span volume spikes dramatically.

Effect:     All services start seeing high throughput. DB starts to queue. P99 climbs
            but errors are low initially. Good for demonstrating volume-based anomaly detection.
"""

import random
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

from .base import BaseScenario
from ..topology import RequestContext, SERVICES
from ..emitter import get_tracer, start_span, http_attrs, db_attrs


class SpanSpikeScenario(BaseScenario):
    name = "span_spike"
    description = "10x traffic surge from marketing campaign — span volume spikes, DB starts queuing"
    affected_service = "api-gateway"
    affected_operation = "GET /api/v1/owners"

    def setup(self, topology=None):
        g = topology.get_service if topology else (lambda role: SERVICES[role])
        self._t_frontend = get_tracer(g("frontend").name,      g("frontend").language,      g("frontend").version)
        self._t_gateway  = get_tracer(g("api-gateway").name,   g("api-gateway").language,   g("api-gateway").version)
        self._t_user     = get_tracer(g("user-service").name,  g("user-service").language,  g("user-service").version)
        self._t_order    = get_tracer(g("order-service").name, g("order-service").language, g("order-service").version)
        self._t_product  = get_tracer(g("product-service").name, g("product-service").language, g("product-service").version)
        self._t_db       = get_tracer(g("postgres").name,      g("postgres").language,      g("postgres").version)
        self._t_cache    = get_tracer(g("redis").name,         g("redis").language,         g("redis").version)

    def run_normal(self, req: RequestContext, parent_ctx: Context) -> None:
        # Emit a single normal request
        self._emit(req, high_load=False)

    def run_anomaly(self, req: RequestContext, parent_ctx: Context) -> None:
        # During the surge emit multiple parallel-style requests per "tick"
        # to simulate higher RPS hitting the same services
        burst = random.randint(3, 6)
        for _ in range(burst):
            r = RequestContext()
            self._emit(r, high_load=True)

    def _emit(self, req: RequestContext, high_load: bool):
        db_ms = random.randint(80, 250) if high_load else random.randint(8, 20)
        svc_ms = random.randint(120, 350) if high_load else random.randint(30, 70)
        db_error = high_load and random.random() < 0.12
        common = {"region": req.region, "customer.tier": req.customer_tier, "user.id": req.user_id}

        endpoint = random.choice(["GET /api/v1/owners", "GET /api/v1/vets", "GET /api/v1/visits"])
        tracer_svc = self._t_user if "owners" in endpoint else (
            self._t_product if "vets" in endpoint else self._t_order)
        op = "owner.get" if "owners" in endpoint else ("vets.list" if "vets" in endpoint else "visit.list")
        table = "owners" if "owners" in endpoint else ("vets" if "vets" in endpoint else "visits")

        with start_span(self._t_frontend, endpoint.replace("/api/v1", ""), duration_ms=12,
                        attrs={**common, **http_attrs("GET", endpoint, 503 if db_error else 200)},
                        error=db_error):

            with start_span(self._t_gateway, endpoint,
                            kind=SpanKind.SERVER, duration_ms=svc_ms,
                            attrs={**common,
                                   **http_attrs("GET", endpoint, 503 if db_error else 200),
                                   "http.request_queue_size": random.randint(50, 200) if high_load else 2}):

                with start_span(tracer_svc, op,
                                kind=SpanKind.SERVER, duration_ms=svc_ms - 20,
                                attrs={**common, **http_attrs("GET", endpoint, 503 if db_error else 200)},
                                error=db_error,
                                error_msg="connection pool timeout under high load"):

                    # Cache check
                    cache_hit = not high_load or random.random() < 0.3
                    with start_span(self._t_cache, f"GET {table}:cache",
                                    kind=SpanKind.CLIENT, duration_ms=1 if cache_hit else 2,
                                    attrs={**common, "db.system": "caffeine", "cache.hit": cache_hit}):
                        pass

                    if not cache_hit:
                        with start_span(self._t_db, f"SELECT {table}",
                                        kind=SpanKind.CLIENT, duration_ms=db_ms,
                                        jitter_ms=30,
                                        attrs={**common,
                                               **db_attrs("h2", "SELECT", table),
                                               "db.connection_pool.wait_ms": random.randint(200, 2000) if high_load else 0},
                                        error=db_error,
                                        error_msg="h2 connection pool exhausted"):
                            pass
