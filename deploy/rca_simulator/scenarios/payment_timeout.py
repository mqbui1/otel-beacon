"""
Scenario: External Payment Gateway Timeout

Root cause: Stripe API latency degrades in us-east-1 (partial outage on their side).
            payment-service has no circuit breaker — it blocks waiting for Stripe.

Effect:     Checkout requests in us-east-1 hang for 30s then fail. Other regions unaffected.

What shows up in Splunk:
  - Tag Spotlight: latency/errors scoped to region=us-east-1 only
  - APM waterfall: payment-service > stripe.charge span takes 28-32s
  - Service Map: payment-service → stripe-api edge turns red
  - Alert: p99 latency > 10s on payment-service
"""

import random
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

from .base import BaseScenario
from ..topology import RequestContext, SERVICES
from ..emitter import get_tracer, start_span, http_attrs, db_attrs


AFFECTED_REGION = "us-east-1"


class PaymentTimeoutScenario(BaseScenario):
    name = "payment_timeout"
    description = "Stripe API partial outage in us-east-1 — payment-service hangs 30s then 504"
    affected_service = "payment-service"
    affected_operation = "stripe.charge"

    def setup(self, topology=None):
        g = topology.get_service if topology else (lambda role: SERVICES[role])
        self._t_frontend  = get_tracer(g("frontend").name,         g("frontend").language,         g("frontend").version)
        self._t_gateway   = get_tracer(g("api-gateway").name,      g("api-gateway").language,      g("api-gateway").version)
        self._t_order     = get_tracer(g("order-service").name,    g("order-service").language,    g("order-service").version)
        self._t_payment   = get_tracer(g("payment-service").name,  g("payment-service").language,  g("payment-service").version)
        self._t_stripe    = get_tracer(g("stripe-api").name,       g("stripe-api").language,       g("stripe-api").version, host="stripe.com")
        self._t_db        = get_tracer(g("postgres").name,         g("postgres").language,         g("postgres").version)
        ov = topology.scenario_overrides.get("payment_timeout", {}) if topology else {}
        self._external_endpoint = ov.get("external_endpoint", "https://api.stripe.com/v1/charges")
        global AFFECTED_REGION
        AFFECTED_REGION = ov.get("affected_region", AFFECTED_REGION)

    def run_normal(self, req: RequestContext, parent_ctx: Context) -> None:
        # Randomly assign a non-affected region
        req.region = random.choice(["us-west-2", "eu-west-1"])
        self._emit(req, stripe_ms=random.randint(200, 600), stripe_error=False)

    def run_anomaly(self, req: RequestContext, parent_ctx: Context) -> None:
        # Affected region only
        req.region = AFFECTED_REGION
        stripe_ms = random.randint(28000, 32000)
        self._emit(req, stripe_ms=stripe_ms, stripe_error=True)

    def _emit(self, req: RequestContext, stripe_ms: int, stripe_error: bool):
        common = {
            "region": req.region,
            "customer.tier": req.customer_tier,
            "user.id": req.user_id,
            "order.id": req.order_id,
        }
        top_error = stripe_error

        with start_span(self._t_frontend, "POST /checkout/confirm", duration_ms=20,
                        attrs={**common, **http_attrs("POST", "/checkout/confirm", 504 if top_error else 200)},
                        error=top_error):

            with start_span(self._t_gateway, "POST /api/v1/payments",
                            kind=SpanKind.SERVER, duration_ms=15,
                            attrs={**common, **http_attrs("POST", "/api/v1/payments", 504 if top_error else 200)},
                            error=top_error):

                # order-service validates the order first (fast)
                with start_span(self._t_order, "order.get",
                                kind=SpanKind.SERVER, duration_ms=25,
                                attrs={**common, **http_attrs("GET", f"/api/v1/orders/{req.order_id}", 200)}):
                    with start_span(self._t_db, "SELECT orders",
                                    kind=SpanKind.CLIENT, duration_ms=12,
                                    attrs={**common, **db_attrs("postgresql", "SELECT", "orders")}):
                        pass

                # payment-service calls Stripe — THIS IS THE SLOW PART
                with start_span(self._t_payment, "payment.charge",
                                kind=SpanKind.SERVER, duration_ms=stripe_ms + 50,
                                attrs={**common, **http_attrs("POST", "/api/v1/payments/charge", 504 if stripe_error else 200)},
                                error=stripe_error, error_msg="timeout waiting for stripe response after 30s"):

                    with start_span(self._t_stripe, "stripe.charge",
                                    kind=SpanKind.CLIENT, duration_ms=stripe_ms,
                                    jitter_ms=1000,
                                    attrs={**common,
                                           "http.method": "POST",
                                           "http.url": "https://api.stripe.com/v1/charges",
                                           "net.peer.name": "api.stripe.com",
                                           "net.peer.port": 443,
                                           "payment.provider": "stripe",
                                           "payment.region": req.region},
                                    error=stripe_error, error_msg="ReadTimeout: HTTPSConnectionPool(host='api.stripe.com'): Read timed out"):
                        pass

                    if not stripe_error:
                        # Record payment in DB
                        with start_span(self._t_db, "INSERT payments",
                                        kind=SpanKind.CLIENT, duration_ms=8,
                                        attrs={**common, **db_attrs("postgresql", "INSERT", "payments")}):
                            pass
