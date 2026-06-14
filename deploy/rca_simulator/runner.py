"""
Scenario runner: generates normal traffic, then injects the anomaly for a window,
then returns to normal. Mimics a real incident timeline for RCA demo.
"""

import logging
import random
import threading
import time
from typing import Optional

from .emitter import flush_all
from .scenarios import SCENARIOS, BaseScenario
from .topology import RequestContext
from .topology_loader import TopologyConfig

logger = logging.getLogger(__name__)


class ScenarioRunner:
    def __init__(
        self,
        scenario_name: str,
        rps: float = 5.0,
        warmup_s: int = 30,
        anomaly_s: int = 120,
        cooldown_s: int = 30,
        anomaly_pct: float = 0.8,
        topology: Optional[TopologyConfig] = None,
    ):
        if scenario_name not in SCENARIOS:
            raise ValueError(f"Unknown scenario '{scenario_name}'. Available: {list(SCENARIOS)}")

        self.scenario: BaseScenario = SCENARIOS[scenario_name]()
        self._topology = topology
        self.rps = rps
        self.warmup_s = warmup_s
        self.anomaly_s = anomaly_s
        self.cooldown_s = cooldown_s
        self.anomaly_pct = anomaly_pct
        self._stop = threading.Event()

    def run(self) -> None:
        self.scenario.setup(topology=self._topology)
        total = self.warmup_s + self.anomaly_s + self.cooldown_s

        print(f"\nScenario: {self.scenario.name}")
        print(f"  {self.scenario.description}")
        print(f"  Warmup: {self.warmup_s}s  |  Anomaly: {self.anomaly_s}s  |  Cooldown: {self.cooldown_s}s")
        print(f"  Target RPS: {self.rps}  |  Total: {total}s\n")

        start = time.monotonic()

        def phase() -> str:
            elapsed = time.monotonic() - start
            if elapsed < self.warmup_s:
                return "warmup"
            if elapsed < self.warmup_s + self.anomaly_s:
                return "anomaly"
            return "cooldown"

        interval = 1.0 / self.rps
        req_count = 0

        while not self._stop.is_set():
            elapsed = time.monotonic() - start
            if elapsed >= total:
                break

            current_phase = phase()
            req = self._random_request(current_phase)

            try:
                t0 = time.monotonic()
                if current_phase == "anomaly" and random.random() < self.anomaly_pct:
                    self.scenario.run_anomaly(req, parent_ctx=None)
                else:
                    self.scenario.run_normal(req, parent_ctx=None)
                req_count += 1

                # Rate limiting: sleep for remaining interval
                elapsed_req = time.monotonic() - t0
                sleep_for = interval - elapsed_req
                if sleep_for > 0:
                    time.sleep(sleep_for)

            except Exception as e:
                logger.warning("Request failed: %s", e)

            # Progress indicator every 10 requests
            if req_count % 10 == 0:
                pct = min(100, int((time.monotonic() - start) / total * 100))
                print(f"  [{pct:3d}%] Phase={current_phase:<8s}  requests={req_count}", flush=True)

        print(f"\nDone. Sent {req_count} requests. Flushing telemetry...", flush=True)
        flush_all()
        print("Flushed.\n")

    def stop(self) -> None:
        self._stop.set()

    @staticmethod
    def _random_request(phase: str) -> RequestContext:
        weights = [0.6, 0.3, 0.1]  # checkout, browse, profile
        request_type = random.choices(
            [RequestContext.checkout, RequestContext.browse, RequestContext.profile],
            weights=weights,
        )[0]
        return request_type()
