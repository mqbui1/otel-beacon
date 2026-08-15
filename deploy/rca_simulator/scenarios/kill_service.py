"""
Scenario: Kill Service

Root cause: visits-service pod is killed (OOM or manual kubectl delete pod).
            No spans emitted from visits-service itself; callers get connection refused.

Effect:     api-gateway shows 503s for all visit-related endpoints. visits-service
            disappears from the service map. missing_service anomaly fires within ~45s.
"""

import random
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

from .base import BaseScenario
from ..topology import RequestContext, SERVICES
from ..emitter import get_tracer, start_span, http_attrs


class KillServiceScenario(BaseScenario):
    name = "kill_service"
    description = "visits-service pod killed — callers get 503 connection refused, service disappears from map"
    affected_service = "order-service"
    affected_operation = "visit.schedule"

    def setup(self, topology=None):
        g = topology.get_service if topology else (lambda role: SERVICES[role])
        self._t_frontend = get_tracer(g("frontend").name,    g("frontend").language,    g("frontend").version)
        self._t_gateway  = get_tracer(g("api-gateway").name, g("api-gateway").language, g("api-gateway").version)
        # Store the topology-resolved service info so run_normal can establish
        # the entity in the registry before the anomaly phase kills it.
        svc = g("order-service")
        self._svc_name = svc.name
        self._svc_lang = svc.language
        self._svc_ver  = svc.version
        ov = topology.scenario_overrides.get("kill_service", {}) if topology else {}
        self._endpoint = ov.get("root_endpoint", "POST /api/v1/visits")

    def run_normal(self, req: RequestContext, parent_ctx: Context) -> None:
        # Normal: visits-service responds fine — emit a full healthy trace
        # using the topology-resolved service name set in setup().
        from ..emitter import get_tracer as _gt
        common = {"region": req.region, "customer.tier": req.customer_tier, "user.id": req.user_id}

        with start_span(self._t_frontend, "POST /visits", duration_ms=15,
                        attrs={**common, **http_attrs("POST", "/visits", 201)}):
            with start_span(self._t_gateway, self._endpoint,
                            kind=SpanKind.SERVER, duration_ms=random.randint(50, 100),
                            attrs={**common, **http_attrs("POST", self._endpoint, 201)}):
                # During normal phase, emit spans from the service that will be "killed"
                # using the topology-resolved name so the entity is in the registry.
                t_order = _gt(self._svc_name, self._svc_lang, self._svc_ver)
                with start_span(t_order, "visit.schedule",
                                kind=SpanKind.SERVER, duration_ms=random.randint(30, 70),
                                attrs={**common, **http_attrs("POST", self._endpoint, 201)}):
                    pass

    def run_anomaly(self, req: RequestContext, parent_ctx: Context) -> None:
        # Anomaly: visits-service is down — only gateway span fires, with 503
        common = {"region": req.region, "customer.tier": req.customer_tier, "user.id": req.user_id}
        connect_timeout = random.randint(3000, 10000)

        with start_span(self._t_frontend, "POST /visits", duration_ms=15,
                        attrs={**common, **http_attrs("POST", "/visits", 503)},
                        error=True):
            with start_span(self._t_gateway, self._endpoint,
                            kind=SpanKind.SERVER, duration_ms=connect_timeout,
                            attrs={**common,
                                   **http_attrs("POST", self._endpoint, 503),
                                   "net.peer.name": "visits-service",
                                   "net.peer.port": 8080,
                                   "error.type": "java.net.ConnectException"},
                            error=True,
                            error_msg="java.net.ConnectException: Connection refused: visits-service/10.0.1.42:8080"):
                pass
        # visits-service emits NO spans during anomaly phase (it's dead)
