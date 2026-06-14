from .base import BaseScenario
from .db_slowdown import DbSlowdownScenario
from .error_cascade import ErrorCascadeScenario
from .payment_timeout import PaymentTimeoutScenario
from .cache_miss_storm import CacheMissStormScenario

SCENARIOS: dict[str, type[BaseScenario]] = {
    "db_slowdown": DbSlowdownScenario,
    "error_cascade": ErrorCascadeScenario,
    "payment_timeout": PaymentTimeoutScenario,
    "cache_miss_storm": CacheMissStormScenario,
}

__all__ = [
    "BaseScenario",
    "SCENARIOS",
    "DbSlowdownScenario",
    "ErrorCascadeScenario",
    "PaymentTimeoutScenario",
    "CacheMissStormScenario",
]
