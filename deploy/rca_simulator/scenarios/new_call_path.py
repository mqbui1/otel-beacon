"""
Scenario: New Call Path

Root cause: A new feature flag in visits-service enables an async call to an
            audit-service (new dependency, never seen before). The call path
            api-gateway → visits-service → audit-service appears for the first time.

Effect:     Trace fingerprint worker detects a structural change in the call graph —
            a new cross-service edge that was never in the baseline. Fires trace_drift
            anomaly. The new call is healthy (no errors), making it a pure structural signal.
"""

import random
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

from .base import BaseScenario
from ..topology import RequestContext, SERVICES, Service
from ..emitter import get_tracer, start_span, http_attrs, db_attrs


class NewCallPathScenario(BaseScenario):
    name = "new_call_path"
    description = "Feature flag enables visits-service → audit-service call — new topology edge triggers trace_drift"
    affected_service = "order-service"
    affected_operation = "visit.schedule"

    def setup(self, topology=None):
        g = topology.get_service if topology else (lambda role: SERVICES[role])
        self._t_frontend = get_tracer(g("frontend").name,      g("frontend").language,      g("frontend").version)
        self._t_gateway  = get_tracer(g("api-gateway").name,   g("api-gateway").language,   g("api-gateway").version)
        self._t_order    = get_tracer(g("order-service").name, g("order-service").language, g("order-service").version)
        self._t_db       = get_tracer(g("postgres").name,      g("postgres").language,      g("postgres").version)
        # audit-service is a NEW dependency not in the baseline topology
        self._t_audit    = get_tracer("audit-service", "java", "1.0.0")
        ov = topology.scenario_overrides.get("new_call_path", {}) if topology else {}
        self._endpoint = ov.get("root_endpoint", "POST /api/v1/visits")

    def run_normal(self, req: RequestContext, parent_ctx: Context) -> None:
        # Normal: no audit-service call — baseline topology
        self._emit(req, with_audit=False)

    def run_anomaly(self, req: RequestContext, parent_ctx: Context) -> None:
        # Anomaly: feature flag enabled — audit-service call added to the path
        self._emit(req, with_audit=True)

    def _emit(self, req: RequestContext, with_audit: bool):
        common = {"region": req.region, "customer.tier": req.customer_tier, "user.id": req.user_id}

        with start_span(self._t_frontend, "POST /visits", duration_ms=15,
                        attrs={**common, **http_attrs("POST", "/visits", 201)}):

            with start_span(self._t_gateway, self._endpoint,
                            kind=SpanKind.SERVER, duration_ms=random.randint(60, 120),
                            attrs={**common, **http_attrs("POST", self._endpoint, 201)}):

                with start_span(self._t_order, "visit.schedule",
                                kind=SpanKind.SERVER, duration_ms=random.randint(40, 80),
                                attrs={**common,
                                       **http_attrs("POST", self._endpoint, 201),
                                       "feature.flag.audit_log": with_audit}):

                    with start_span(self._t_db, "INSERT visits",
                                    kind=SpanKind.CLIENT, duration_ms=random.randint(5, 15),
                                    attrs={**common, **db_attrs("h2", "INSERT", "visits",
                                           "INSERT INTO visits (date, description, pet_id) VALUES (?, ?, ?)")}):
                        pass

                    if with_audit:
                        # NEW edge: visits-service → audit-service (never in baseline)
                        with start_span(self._t_audit, "audit.log",
                                        kind=SpanKind.SERVER, duration_ms=random.randint(8, 20),
                                        attrs={**common,
                                               **http_attrs("POST", "/api/v1/audit/events", 200),
                                               "audit.event_type": "visit_created",
                                               "audit.entity": "visit"}):
                            pass
