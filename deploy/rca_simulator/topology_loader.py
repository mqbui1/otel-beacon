"""
Loads a prospect-specific topology YAML and returns overridden Service objects
that scenarios use in place of the default SERVICES registry.

YAML format:
  environment: acme-staging
  services:
    frontend:      { name: acme-web,        language: nodejs,   version: "4.1.2" }
    api-gateway:   { name: acme-api,         language: java,     version: "2.0.0" }
    order-service: { name: fulfillment-svc,  language: java,     version: "1.9.3" }
    database:      { name: aurora-mysql,     language: mysql,    version: "8.0"   }
    cache:         { name: elasticache,      language: redis,    version: "6.2"   }
    external-api:  { name: fedex-api,        language: external, version: "v3"    }

  scenario_overrides:
    db_slowdown:
      root_endpoint: "POST /api/fulfillment/orders"
      slow_query: "SELECT * FROM fulfillment_orders WHERE account_id = ?"
      slow_table: "fulfillment_orders"
    payment_timeout:
      external_endpoint: "https://apis.fedex.com/ship/v1/shipments"
      affected_region: "us-east-1"
"""

from __future__ import annotations

import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .topology import Service, SERVICES


@dataclass
class TopologyConfig:
    environment: str = "rca-demo"
    # role (e.g. "order-service") -> overridden Service
    services: dict[str, Service] = field(default_factory=dict)
    # scenario name -> dict of override kwargs
    scenario_overrides: dict[str, dict] = field(default_factory=dict)

    def get_service(self, role: str) -> Service:
        """Return the prospect's service for this role, falling back to the default."""
        return self.services.get(role, SERVICES.get(role, Service(role, "unknown", "1.0.0")))

    def get_override(self, scenario: str, key: str, default=None):
        return self.scenario_overrides.get(scenario, {}).get(key, default)


def load_topology(path: str) -> TopologyConfig:
    data = yaml.safe_load(Path(path).read_text())

    cfg = TopologyConfig(environment=data.get("environment", "rca-demo"))

    for role, svc_data in data.get("services", {}).items():
        cfg.services[role] = Service(
            name=svc_data["name"],
            language=svc_data.get("language", "unknown"),
            version=str(svc_data.get("version", "1.0.0")),
            hosts=svc_data.get("hosts", []),
        )

    cfg.scenario_overrides = data.get("scenario_overrides", {})

    return cfg


# Singleton — populated by __main__ if --topology-file is passed
_active: Optional[TopologyConfig] = None


def get_active() -> TopologyConfig:
    return _active or TopologyConfig()


def set_active(cfg: TopologyConfig) -> None:
    global _active
    _active = cfg
