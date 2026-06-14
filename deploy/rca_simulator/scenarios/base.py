"""Base class for all RCA scenarios."""

from abc import ABC, abstractmethod
from opentelemetry.context import Context
from ..topology import RequestContext


class BaseScenario(ABC):
    name: str = ""
    description: str = ""
    affected_service: str = ""
    affected_operation: str = ""

    @abstractmethod
    def run_normal(self, req: RequestContext, parent_ctx: Context) -> None:
        """Emit one normal (healthy) trace for this scenario's request type."""

    @abstractmethod
    def run_anomaly(self, req: RequestContext, parent_ctx: Context) -> None:
        """Emit one anomalous trace — the failure being demonstrated."""

    def setup(self, topology=None) -> None:
        """Called once before the scenario run starts. topology is a TopologyConfig or None."""
