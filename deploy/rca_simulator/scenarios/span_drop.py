"""
Scenario: Span Drop

Root cause: OTel Java agent on visits-service is misconfigured — sampler flipped to
            ParentBased(TraceIdRatioBased(0.1)). 90% of spans are silently dropped.

Effect:     visits-service appears in topology but has near-zero span count. Throughput
            metrics look fine, but tracing coverage collapses — blind spot for RCA.
"""

import random
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

from .base import BaseScenario
from ..topology import RequestContext, SERVICES
from ..emitter import get_tracer, start_span, http_attrs, db_attrs


class SpanDropScenario(BaseScenario):
    name = "span_drop"
    description = "OTel agent sampling misconfiguration on visits-service — 90% of spans silently dropped"
    affected_service = "order-service"
    affected_operation = "visit.schedule"

    # Drop rate during anomaly phase — only 10% of requests actually emit spans
    DROP_RATE = 0.90

    def setup(self, topology=None):
        g = topology.get_service if topology else (lambda role: SERVICES[role])
        self._t_frontend = get_tracer(g("frontend").name,      g("frontend").language,      g("frontend").version)
        self._t_gateway  = get_tracer(g("api-gateway").name,   g("api-gateway").language,   g("api-gateway").version)
        self._t_order    = get_tracer(g("order-service").name, g("order-service").language, g("order-service").version)
        self._t_db       = get_tracer(g("postgres").name,      g("postgres").language,      g("postgres").version)
        ov = topology.scenario_overrides.get("span_drop", {}) if topology else {}
        self._endpoint = ov.get("root_endpoint", "POST /api/v1/visits")

    def run_normal(self, req: RequestContext, parent_ctx: Context) -> None:
        self._emit(req, drop=False)

    def run_anomaly(self, req: RequestContext, parent_ctx: Context) -> None:
        # Simulate sampler: gateway still emits (it uses a different agent config),
        # but visits-service spans are dropped most of the time.
        drop = random.random() < self.DROP_RATE
        self._emit(req, drop=drop)

    def _emit(self, req: RequestContext, drop: bool):
        common = {"region": req.region, "customer.tier": req.customer_tier, "user.id": req.user_id}

        # Gateway always emits
        with start_span(self._t_frontend, "POST /visits", duration_ms=15,
                        attrs={**common, **http_attrs("POST", "/visits", 200)}):

            with start_span(self._t_gateway, self._endpoint,
                            kind=SpanKind.SERVER, duration_ms=random.randint(50, 120),
                            attrs={**common, **http_attrs("POST", self._endpoint, 200)}):

                if not drop:
                    # visits-service span only emitted when not dropped
                    with start_span(self._t_order, "visit.schedule",
                                    kind=SpanKind.SERVER, duration_ms=random.randint(40, 90),
                                    attrs={**common,
                                           **http_attrs("POST", self._endpoint, 201),
                                           "otel.sampler": "TraceIdRatioBased",
                                           "otel.sample_rate": 0.1 if drop else 1.0}):

                        with start_span(self._t_db, "INSERT visits",
                                        kind=SpanKind.CLIENT, duration_ms=random.randint(5, 15),
                                        attrs={**common, **db_attrs("h2", "INSERT", "visits",
                                               "INSERT INTO visits (date, description, pet_id) VALUES (?, ?, ?)")}):
                            pass
