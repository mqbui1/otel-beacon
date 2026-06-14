"""
Scenario: OOM Crash

Root cause: vets-service (product-service) leaks memory loading vet specialties into a
            static list on every request. JVM heap fills up → OutOfMemoryError → pod restarts.

Effect:     vets-service alternates between error bursts (during OOM) and brief healthy windows
            (after restart). Service map shows vets-service flapping red/green.
"""

import random
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

from .base import BaseScenario
from ..topology import RequestContext, SERVICES
from ..emitter import get_tracer, start_span, http_attrs, db_attrs


class OomCrashScenario(BaseScenario):
    name = "oom_crash"
    description = "vets-service JVM heap exhaustion → OutOfMemoryError → pod restart loop"
    affected_service = "product-service"
    affected_operation = "vets.list"

    def setup(self, topology=None):
        g = topology.get_service if topology else (lambda role: SERVICES[role])
        self._t_frontend = get_tracer(g("frontend").name,        g("frontend").language,        g("frontend").version)
        self._t_gateway  = get_tracer(g("api-gateway").name,     g("api-gateway").language,     g("api-gateway").version)
        self._t_product  = get_tracer(g("product-service").name, g("product-service").language, g("product-service").version)
        self._t_db       = get_tracer(g("postgres").name,        g("postgres").language,        g("postgres").version)
        ov = topology.scenario_overrides.get("oom_crash", {}) if topology else {}
        self._endpoint = ov.get("root_endpoint", "GET /api/v1/vets")
        self._req_count = 0

    def run_normal(self, req: RequestContext, parent_ctx: Context) -> None:
        req.http_method = "GET"
        self._emit(req, heap_pct=random.randint(30, 55), oom=False)

    def run_anomaly(self, req: RequestContext, parent_ctx: Context) -> None:
        req.http_method = "GET"
        self._req_count += 1
        # Simulate heap climbing then OOM burst then brief recovery (restart)
        cycle = self._req_count % 20
        if cycle < 12:
            heap_pct = 60 + cycle * 3  # 60% → 93%
            oom = heap_pct > 88
        else:
            heap_pct = 35  # recovering after restart
            oom = False
        self._emit(req, heap_pct=heap_pct, oom=oom)

    def _emit(self, req: RequestContext, heap_pct: int, oom: bool):
        common = {"region": req.region, "customer.tier": req.customer_tier, "user.id": req.user_id}
        svc_ms = random.randint(800, 2000) if oom else random.randint(30, 70)

        with start_span(self._t_frontend, "GET /vets", duration_ms=15,
                        attrs={**common, **http_attrs("GET", "/vets", 503 if oom else 200)},
                        error=oom):

            with start_span(self._t_gateway, self._endpoint,
                            kind=SpanKind.SERVER, duration_ms=12,
                            attrs={**common, **http_attrs("GET", self._endpoint, 503 if oom else 200)},
                            error=oom):

                with start_span(self._t_product, "vets.list",
                                kind=SpanKind.SERVER, duration_ms=svc_ms,
                                attrs={**common,
                                       **http_attrs("GET", self._endpoint, 503 if oom else 200),
                                       "jvm.memory.heap.used_pct": heap_pct,
                                       "jvm.memory.heap.used_mb": int(heap_pct * 5.12),
                                       "jvm.memory.heap.max_mb": 512},
                                error=oom,
                                error_msg="java.lang.OutOfMemoryError: Java heap space — vet specialties accumulation"):

                    if not oom:
                        with start_span(self._t_db, "SELECT vets",
                                        kind=SpanKind.CLIENT, duration_ms=random.randint(8, 25),
                                        attrs={**common, **db_attrs("h2", "SELECT", "vets",
                                               "SELECT v.*, s.name AS specialty FROM vets v LEFT JOIN specialties s ON v.id = s.vet_id")}):
                            pass
