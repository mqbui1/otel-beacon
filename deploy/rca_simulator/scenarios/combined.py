"""
Scenario: Combined Signals

Root cause: Simultaneous incidents — visits-service has a latency spike (thread starvation)
            AND vets-service has an elevated error rate (config flag deployed to wrong env).

Effect:     Multiple independent red signals fire at once. Harder to triage — no single root
            cause. Tests the incident grouping / deduplication logic.
"""

import random
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

from .base import BaseScenario
from ..topology import RequestContext, SERVICES
from ..emitter import get_tracer, start_span, http_attrs, db_attrs


class CombinedScenario(BaseScenario):
    name = "combined"
    description = "Simultaneous latency spike (visits-service) + error rate spike (vets-service)"
    affected_service = "api-gateway"
    affected_operation = "multiple"

    def setup(self, topology=None):
        g = topology.get_service if topology else (lambda role: SERVICES[role])
        self._t_frontend = get_tracer(g("frontend").name,        g("frontend").language,        g("frontend").version)
        self._t_gateway  = get_tracer(g("api-gateway").name,     g("api-gateway").language,     g("api-gateway").version)
        self._t_order    = get_tracer(g("order-service").name,   g("order-service").language,   g("order-service").version)
        self._t_product  = get_tracer(g("product-service").name, g("product-service").language, g("product-service").version)
        self._t_user     = get_tracer(g("user-service").name,    g("user-service").language,    g("user-service").version)
        self._t_db       = get_tracer(g("postgres").name,        g("postgres").language,        g("postgres").version)

    def run_normal(self, req: RequestContext, parent_ctx: Context) -> None:
        if random.random() < 0.5:
            self._emit_visit(req, slow=False, vets_error=False)
        else:
            self._emit_vets(req, vets_error=False)

    def run_anomaly(self, req: RequestContext, parent_ctx: Context) -> None:
        if random.random() < 0.5:
            self._emit_visit(req, slow=True, vets_error=False)
        else:
            self._emit_vets(req, vets_error=random.random() < 0.6)

    def _emit_visit(self, req: RequestContext, slow: bool, vets_error: bool):
        common = {"region": req.region, "customer.tier": req.customer_tier, "user.id": req.user_id}
        svc_ms = random.randint(2500, 5000) if slow else random.randint(40, 90)

        with start_span(self._t_frontend, "POST /visits", duration_ms=15,
                        attrs={**common, **http_attrs("POST", "/visits", 200)}):
            with start_span(self._t_gateway, "POST /api/v1/visits",
                            kind=SpanKind.SERVER, duration_ms=svc_ms + 10,
                            attrs={**common, **http_attrs("POST", "/api/v1/visits", 200)}):
                with start_span(self._t_order, "visit.schedule",
                                kind=SpanKind.SERVER, duration_ms=svc_ms,
                                attrs={**common,
                                       **http_attrs("POST", "/api/v1/visits", 200),
                                       "thread.pool.queue_depth": random.randint(15, 30) if slow else 1}):
                    with start_span(self._t_db, "INSERT visits",
                                    kind=SpanKind.CLIENT, duration_ms=random.randint(6, 18),
                                    attrs={**common, **db_attrs("h2", "INSERT", "visits")}):
                        pass

    def _emit_vets(self, req: RequestContext, vets_error: bool):
        common = {"region": req.region, "customer.tier": req.customer_tier, "user.id": req.user_id}

        with start_span(self._t_frontend, "GET /vets", duration_ms=12,
                        attrs={**common, **http_attrs("GET", "/vets", 500 if vets_error else 200)},
                        error=vets_error):
            with start_span(self._t_gateway, "GET /api/v1/vets",
                            kind=SpanKind.SERVER, duration_ms=10,
                            attrs={**common, **http_attrs("GET", "/api/v1/vets", 500 if vets_error else 200)},
                            error=vets_error):
                with start_span(self._t_product, "vets.list",
                                kind=SpanKind.SERVER, duration_ms=random.randint(25, 60),
                                attrs={**common,
                                       **http_attrs("GET", "/api/v1/vets", 500 if vets_error else 200),
                                       "feature.flag": "new_vet_api_v2",
                                       "feature.flag.env": "prod-wrong"},
                                error=vets_error,
                                error_msg="IllegalStateException: feature flag 'new_vet_api_v2' "
                                          "enabled on wrong environment — specialty mapping is null"):
                    if not vets_error:
                        with start_span(self._t_db, "SELECT vets",
                                        kind=SpanKind.CLIENT, duration_ms=random.randint(8, 20),
                                        attrs={**common, **db_attrs("h2", "SELECT", "vets")}):
                            pass
