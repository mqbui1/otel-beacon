from .base import BaseScenario
from .db_slowdown import DbSlowdownScenario
from .error_cascade import ErrorCascadeScenario
from .payment_timeout import PaymentTimeoutScenario
from .cache_miss_storm import CacheMissStormScenario
from .latency_spike import LatencySpikeScenario
from .oom_crash import OomCrashScenario
from .cascading_failure import CascadingFailureScenario
from .new_error_sig import NewErrorSigScenario
from .span_drop import SpanDropScenario
from .span_spike import SpanSpikeScenario
from .combined import CombinedScenario
from .kill_service import KillServiceScenario

SCENARIOS: dict[str, type[BaseScenario]] = {
    "db_slowdown":        DbSlowdownScenario,
    "error_cascade":      ErrorCascadeScenario,
    "payment_timeout":    PaymentTimeoutScenario,
    "cache_miss_storm":   CacheMissStormScenario,
    "latency_spike":      LatencySpikeScenario,
    "oom_crash":          OomCrashScenario,
    "cascading_failure":  CascadingFailureScenario,
    "new_error_sig":      NewErrorSigScenario,
    "span_drop":          SpanDropScenario,
    "span_spike":         SpanSpikeScenario,
    "combined":           CombinedScenario,
    "kill_service":       KillServiceScenario,
}

__all__ = [
    "BaseScenario",
    "SCENARIOS",
    "DbSlowdownScenario",
    "ErrorCascadeScenario",
    "PaymentTimeoutScenario",
    "CacheMissStormScenario",
    "LatencySpikeScenario",
    "OomCrashScenario",
    "CascadingFailureScenario",
    "NewErrorSigScenario",
    "SpanDropScenario",
    "SpanSpikeScenario",
    "CombinedScenario",
    "KillServiceScenario",
]
