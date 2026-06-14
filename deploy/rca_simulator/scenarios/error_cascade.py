"""
Scenario: Error Cascade

Root cause: user-service v2.3.1 has a bug — JWT validation throws NullPointerException
            when the token contains a new 'org_id' field (rolled out to enterprise tier only).

Effect:     user-service returns 500 for enterprise users → order-service retries 3x →
            DB connection pool exhausted → all tiers start seeing errors.

What shows up in Splunk:
  - Tag Spotlight: errors first appear on customer.tier=enterprise, then spread
  - APM: NullPointerException stack trace in user-service span events
  - Service Map: user-service turns red, order-service follows
  - Detector fires on error rate > 1%
"""

import random
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

from .base import BaseScenario
from ..topology import RequestContext, SERVICES
from ..emitter import get_tracer, start_span, http_attrs, db_attrs


class ErrorCascadeScenario(BaseScenario):
    name = "error_cascade"
    description = "JWT bug in user-service causes 500s for enterprise tier → cascades to all users"
    affected_service = "user-service"
    affected_operation = "user.validate"

    def setup(self, topology=None):
        g = topology.get_service if topology else (lambda role: SERVICES[role])
        self._t_frontend = get_tracer(g("frontend").name,      g("frontend").language,      g("frontend").version)
        self._t_gateway  = get_tracer(g("api-gateway").name,   g("api-gateway").language,   g("api-gateway").version)
        self._t_order    = get_tracer(g("order-service").name,  g("order-service").language, g("order-service").version)
        self._t_user     = get_tracer(g("user-service").name,  g("user-service").language,  g("user-service").version)
        self._t_db       = get_tracer(g("postgres").name,      g("postgres").language,      g("postgres").version)

    def run_normal(self, req: RequestContext, parent_ctx: Context) -> None:
        # Force non-enterprise for normal traffic
        req.customer_tier = random.choice(["free", "pro"])
        self._emit(req, user_error=False, db_pool_exhausted=False)

    def run_anomaly(self, req: RequestContext, parent_ctx: Context) -> None:
        # Anomaly: enterprise users hit the bug; connection pool fills up and spills to others
        is_enterprise = req.customer_tier == "enterprise" or random.random() < 0.4
        req.customer_tier = "enterprise" if is_enterprise else req.customer_tier
        db_exhausted = random.random() < 0.3  # pool exhaustion spreads to non-enterprise too
        self._emit(req, user_error=is_enterprise, db_pool_exhausted=db_exhausted)

    def _emit(self, req: RequestContext, user_error: bool, db_pool_exhausted: bool):
        common = {
            "region": req.region,
            "customer.tier": req.customer_tier,
            "user.id": req.user_id,
        }
        top_level_error = user_error or db_pool_exhausted

        with start_span(self._t_frontend, "GET /checkout", duration_ms=18,
                        attrs={**common, **http_attrs("GET", "/checkout", 500 if top_level_error else 200)},
                        error=top_level_error, error_msg="upstream service failure"):

            with start_span(self._t_gateway, "POST /api/v1/orders",
                            kind=SpanKind.SERVER, duration_ms=12,
                            attrs={**common, **http_attrs("POST", "/api/v1/orders", 500 if top_level_error else 200)},
                            error=top_level_error):

                # user-service: throws NPE for enterprise JWT tokens
                with start_span(self._t_user, "user.validate",
                                kind=SpanKind.SERVER, duration_ms=15,
                                attrs={**common,
                                       **http_attrs("POST", "/api/v1/users/validate", 500 if user_error else 200),
                                       "service.version": "2.3.1"},
                                error=user_error,
                                error_msg="NullPointerException: token.getClaims().get('org_id') is null"):
                    pass

                if not user_error:
                    # order-service — DB pool exhaustion from retry storms
                    retries = 3 if db_pool_exhausted else 1
                    for attempt in range(retries):
                        is_last = attempt == retries - 1
                        with start_span(self._t_order, "order.create",
                                        kind=SpanKind.SERVER,
                                        duration_ms=random.randint(200, 800) if db_pool_exhausted else random.randint(30, 80),
                                        attrs={**common,
                                               **http_attrs("POST", "/api/v1/orders", 503 if (db_pool_exhausted and is_last) else 201),
                                               "retry.attempt": attempt},
                                        error=db_pool_exhausted and is_last,
                                        error_msg="HikariPool-1 - Connection is not available, request timed out after 30000ms"):

                            with start_span(self._t_db, "SELECT users",
                                            kind=SpanKind.CLIENT,
                                            duration_ms=random.randint(500, 2000) if db_pool_exhausted else 10,
                                            attrs={**common,
                                                   **db_attrs("postgresql", "SELECT", "users"),
                                                   "db.connection_pool.wait_ms": random.randint(5000, 30000) if db_pool_exhausted else 2},
                                            error=db_pool_exhausted and is_last,
                                            error_msg="connection pool exhausted"):
                                pass
