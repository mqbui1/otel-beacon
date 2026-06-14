"""
Scenario: New Error Signature

Root cause: A new code path in customers-service (v2.4.0) throws an unhandled
            AuthorizationException for users with the 'admin' role — never seen before.

Effect:     Error rate stays low (only admin users hit it), but a brand-new exception
            class appears in traces. RCA requires inspecting span events, not just error rate.
"""

import random
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

from .base import BaseScenario
from ..topology import RequestContext, SERVICES
from ..emitter import get_tracer, start_span, http_attrs, db_attrs


class NewErrorSigScenario(BaseScenario):
    name = "new_error_sig"
    description = "customers-service v2.4.0 throws unhandled AuthorizationException for admin role"
    affected_service = "user-service"
    affected_operation = "owner.update"

    def setup(self, topology=None):
        g = topology.get_service if topology else (lambda role: SERVICES[role])
        self._t_frontend = get_tracer(g("frontend").name,     g("frontend").language,     g("frontend").version)
        self._t_gateway  = get_tracer(g("api-gateway").name,  g("api-gateway").language,  g("api-gateway").version)
        self._t_user     = get_tracer(g("user-service").name, g("user-service").language, g("user-service").version)
        self._t_db       = get_tracer(g("postgres").name,     g("postgres").language,     g("postgres").version)
        ov = topology.scenario_overrides.get("new_error_sig", {}) if topology else {}
        self._endpoint = ov.get("root_endpoint", "PUT /api/v1/owners/:id")

    def run_normal(self, req: RequestContext, parent_ctx: Context) -> None:
        req.http_method = "PUT"
        req.customer_tier = random.choice(["free", "pro"])
        self._emit(req, new_error=False)

    def run_anomaly(self, req: RequestContext, parent_ctx: Context) -> None:
        req.http_method = "PUT"
        # ~20% of requests hit the admin code path
        is_admin = random.random() < 0.2
        req.customer_tier = "admin" if is_admin else random.choice(["free", "pro"])
        self._emit(req, new_error=is_admin)

    def _emit(self, req: RequestContext, new_error: bool):
        common = {
            "region": req.region,
            "customer.tier": req.customer_tier,
            "user.id": req.user_id,
            "user.role": "admin" if new_error else "user",
        }

        with start_span(self._t_frontend, "PUT /owners/:id", duration_ms=15,
                        attrs={**common, **http_attrs("PUT", "/owners/:id", 500 if new_error else 200)},
                        error=new_error):

            with start_span(self._t_gateway, self._endpoint,
                            kind=SpanKind.SERVER, duration_ms=12,
                            attrs={**common, **http_attrs("PUT", self._endpoint, 500 if new_error else 200)},
                            error=new_error):

                with start_span(self._t_user, "owner.update",
                                kind=SpanKind.SERVER, duration_ms=random.randint(30, 70),
                                attrs={**common,
                                       **http_attrs("PUT", self._endpoint, 500 if new_error else 200),
                                       "service.version": "2.4.0"},
                                error=new_error,
                                error_msg="com.petclinic.security.AuthorizationException: "
                                          "role 'admin' is not permitted to call owner.update — "
                                          "missing SCOPE_owners:write grant (introduced in v2.4.0)"):

                    if not new_error:
                        with start_span(self._t_db, "UPDATE owners",
                                        kind=SpanKind.CLIENT, duration_ms=random.randint(5, 15),
                                        attrs={**common,
                                               **db_attrs("h2", "UPDATE", "owners",
                                                          "UPDATE owners SET first_name=?, last_name=? WHERE id=?")}):
                            pass
