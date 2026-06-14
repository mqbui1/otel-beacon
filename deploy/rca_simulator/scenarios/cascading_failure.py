"""
Scenario: Cascading Failure

Root cause: h2 database becomes unavailable (disk full). customers-service fails first,
            then api-gateway starts returning 500s to all callers.

Effect:     All services that depend on customers-service start failing. The failure
            propagates upward — api-gateway error rate climbs even though it is healthy.
"""

import random
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

from .base import BaseScenario
from ..topology import RequestContext, SERVICES
from ..emitter import get_tracer, start_span, http_attrs, db_attrs


class CascadingFailureScenario(BaseScenario):
    name = "cascading_failure"
    description = "h2 DB unavailable → customers-service fails → api-gateway cascades to all callers"
    affected_service = "user-service"
    affected_operation = "owner.get"

    def setup(self, topology=None):
        g = topology.get_service if topology else (lambda role: SERVICES[role])
        self._t_frontend = get_tracer(g("frontend").name,      g("frontend").language,      g("frontend").version)
        self._t_gateway  = get_tracer(g("api-gateway").name,   g("api-gateway").language,   g("api-gateway").version)
        self._t_user     = get_tracer(g("user-service").name,  g("user-service").language,  g("user-service").version)
        self._t_order    = get_tracer(g("order-service").name, g("order-service").language, g("order-service").version)
        self._t_db       = get_tracer(g("postgres").name,      g("postgres").language,      g("postgres").version)
        ov = topology.scenario_overrides.get("cascading_failure", {}) if topology else {}
        self._endpoint = ov.get("root_endpoint", "GET /api/v1/owners")

    def run_normal(self, req: RequestContext, parent_ctx: Context) -> None:
        req.http_method = "GET"
        self._emit(req, db_down=False)

    def run_anomaly(self, req: RequestContext, parent_ctx: Context) -> None:
        req.http_method = "GET"
        self._emit(req, db_down=True)

    def _emit(self, req: RequestContext, db_down: bool):
        common = {"region": req.region, "customer.tier": req.customer_tier, "user.id": req.user_id}
        db_ms = random.randint(5000, 10000) if db_down else random.randint(8, 20)
        user_error = db_down
        cascade_error = db_down and random.random() < 0.7

        with start_span(self._t_frontend, "GET /owners", duration_ms=18,
                        attrs={**common, **http_attrs("GET", "/owners", 500 if cascade_error else 200)},
                        error=cascade_error):

            with start_span(self._t_gateway, self._endpoint,
                            kind=SpanKind.SERVER, duration_ms=15,
                            attrs={**common, **http_attrs("GET", self._endpoint, 500 if cascade_error else 200)},
                            error=cascade_error,
                            error_msg="upstream customers-service unavailable"):

                # customers-service — DB call fails
                with start_span(self._t_user, "owner.get",
                                kind=SpanKind.SERVER, duration_ms=db_ms + 10,
                                attrs={**common, **http_attrs("GET", self._endpoint, 500 if user_error else 200)},
                                error=user_error,
                                error_msg="DataAccessException: could not execute query — connection to h2 lost"):

                    with start_span(self._t_db, "SELECT owners",
                                    kind=SpanKind.CLIENT, duration_ms=db_ms,
                                    jitter_ms=200,
                                    attrs={**common,
                                           **db_attrs("h2", "SELECT", "owners",
                                                      "SELECT * FROM owners WHERE last_name LIKE ?"),
                                           "db.connection.error": "No suitable driver found" if db_down else "",
                                           "host.name": "petclinic-db"},
                                    error=db_down,
                                    error_msg="Connection refused: petclinic-db:9092 — disk quota exceeded"):
                        pass

                # visits-service also affected (secondary cascade)
                if cascade_error:
                    with start_span(self._t_order, "visit.list",
                                    kind=SpanKind.SERVER, duration_ms=random.randint(20, 60),
                                    attrs={**common, **http_attrs("GET", "/api/v1/visits", 503)},
                                    error=True,
                                    error_msg="upstream dependency customers-service returned 500"):
                        pass
