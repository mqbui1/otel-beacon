"""
Scenario: Latency Spike

Root cause: CPU saturation on visits-service host (e.g. GC pressure or thread-pool starvation).
            All requests slow down uniformly — no errors, but P99 goes from ~80ms to 3-6s.

Effect:     api-gateway starts queuing requests; frontend users see slow page loads.
            No error rate increase — makes RCA harder (no obvious red signal).
"""

import random
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

from .base import BaseScenario
from ..topology import RequestContext, SERVICES
from ..emitter import get_tracer, start_span, http_attrs, db_attrs


class LatencySpikeScenario(BaseScenario):
    name = "latency_spike"
    description = "CPU saturation on visits-service — P99 spikes 3-6s, no errors (silent degradation)"
    affected_service = "order-service"
    affected_operation = "visit.schedule"

    def setup(self, topology=None):
        g = topology.get_service if topology else (lambda role: SERVICES[role])
        self._t_frontend = get_tracer(g("frontend").name,      g("frontend").language,      g("frontend").version)
        self._t_gateway  = get_tracer(g("api-gateway").name,   g("api-gateway").language,   g("api-gateway").version)
        self._t_order    = get_tracer(g("order-service").name, g("order-service").language, g("order-service").version)
        self._t_db       = get_tracer(g("postgres").name,      g("postgres").language,      g("postgres").version)
        ov = topology.scenario_overrides.get("latency_spike", {}) if topology else {}
        self._endpoint = ov.get("root_endpoint", "POST /api/v1/visits")

    def run_normal(self, req: RequestContext, parent_ctx: Context) -> None:
        self._emit(req, service_ms=random.randint(40, 90))

    def run_anomaly(self, req: RequestContext, parent_ctx: Context) -> None:
        self._emit(req, service_ms=random.randint(2800, 5800))

    def _emit(self, req: RequestContext, service_ms: int):
        common = {"region": req.region, "customer.tier": req.customer_tier, "user.id": req.user_id}
        gw_ms = min(service_ms + 10, service_ms + random.randint(5, 20))

        with start_span(self._t_frontend, "GET /visits", duration_ms=20,
                        attrs={**common, **http_attrs("GET", "/visits", 200)}):

            with start_span(self._t_gateway, self._endpoint,
                            kind=SpanKind.SERVER, duration_ms=gw_ms,
                            attrs={**common, **http_attrs("POST", self._endpoint, 200)}):

                with start_span(self._t_order, "visit.schedule",
                                kind=SpanKind.SERVER, duration_ms=service_ms,
                                attrs={**common,
                                       **http_attrs("POST", self._endpoint, 200),
                                       "thread.pool.active": random.randint(18, 20) if service_ms > 500 else random.randint(2, 8),
                                       "jvm.gc.pause_ms": random.randint(500, 2000) if service_ms > 500 else random.randint(2, 15)}):

                    with start_span(self._t_db, "SELECT visits",
                                    kind=SpanKind.CLIENT, duration_ms=random.randint(8, 20),
                                    attrs={**common, **db_attrs("h2", "SELECT", "visits",
                                           "SELECT * FROM visits WHERE pet_id = ?")}):
                        pass
