"""
Service topology for RCA simulator.

Defines a realistic e-commerce microservice graph. Each service has a name,
language tag (for span attributes), and version (to enable Tag Spotlight filtering
by version during demos).
"""

from dataclasses import dataclass, field
from typing import Optional
import random

REGIONS = ["us-east-1", "us-west-2", "eu-west-1"]
CUSTOMER_TIERS = ["free", "pro", "enterprise"]
HTTP_METHODS = ["GET", "POST", "PUT", "DELETE"]


@dataclass
class Service:
    name: str
    language: str
    version: str
    # Host pool — simulator picks one per request to make infra correlation realistic
    hosts: list[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.hosts:
            self.hosts = [f"{self.name}-{i}" for i in range(1, 4)]

    def random_host(self) -> str:
        return random.choice(self.hosts)


# Service registry
SERVICES: dict[str, Service] = {
    "frontend":             Service("frontend",             "nodejs",     "3.2.1"),
    "api-gateway":          Service("api-gateway",          "java",       "2.1.0"),
    "order-service":        Service("order-service",        "java",       "4.0.2"),
    "product-service":      Service("product-service",      "python",     "1.5.0"),
    "user-service":         Service("user-service",         "java",       "2.3.1"),
    "payment-service":      Service("payment-service",      "java",       "1.2.0"),
    "notification-service": Service("notification-service", "python",     "0.9.3"),
    "inventory-service":    Service("inventory-service",    "go",         "1.1.0"),
    "postgres":             Service("postgres",             "postgresql",  "14.2", hosts=["db-primary", "db-replica-1"]),
    "redis":                Service("redis",                "redis",       "7.0",  hosts=["redis-primary"]),
    "stripe-api":           Service("stripe-api",           "external",    "v1",   hosts=["stripe.com"]),
}


@dataclass
class RequestContext:
    """Per-request metadata — used as span attributes for Tag Spotlight filtering."""
    region: str = field(default_factory=lambda: random.choice(REGIONS))
    customer_tier: str = field(default_factory=lambda: random.choice(CUSTOMER_TIERS))
    user_id: str = field(default_factory=lambda: f"user-{random.randint(1000, 9999)}")
    order_id: str = field(default_factory=lambda: f"order-{random.randint(10000, 99999)}")
    http_method: str = "POST"
    endpoint: str = "/api/v1/orders"

    @classmethod
    def checkout(cls) -> "RequestContext":
        return cls(http_method="POST", endpoint="/api/v1/orders")

    @classmethod
    def browse(cls) -> "RequestContext":
        return cls(http_method="GET", endpoint="/api/v1/products")

    @classmethod
    def profile(cls) -> "RequestContext":
        return cls(http_method="GET", endpoint="/api/v1/users/me")
