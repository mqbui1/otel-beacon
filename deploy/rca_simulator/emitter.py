"""
OTel setup and span/metric/log helpers.

Each service gets its own Tracer so spans carry the correct service.name resource.
We use one MeterProvider shared across all services for metrics.
"""

import logging
import os
import random
import threading
import time
from contextlib import contextmanager
from typing import Optional

from opentelemetry import trace, metrics
from opentelemetry import context as otel_context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.trace import SpanKind, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# ---------------------------------------------------------------------------
# Thread-local time offset — used by the baseline seeder to emit spans with
# past timestamps without real sleeps.  0 = real-time (default).
# ---------------------------------------------------------------------------
_span_time_offset = threading.local()

def set_time_offset(seconds: float) -> None:
    """Set how many seconds in the past new spans should be timestamped."""
    _span_time_offset.value = seconds

def get_time_offset() -> float:
    return getattr(_span_time_offset, 'value', 0.0)

logger = logging.getLogger(__name__)

_propagator = TraceContextTextMapPropagator()

# Registry: service_name -> TracerProvider
_providers: dict[str, TracerProvider] = {}
_tracers: dict[str, trace.Tracer] = {}
_meter_provider: Optional[MeterProvider] = None
_meter = None


def _make_resource(service_name: str, language: str, version: str, host: str, env: str) -> Resource:
    return Resource.create({
        "service.name": service_name,
        "service.version": version,
        "telemetry.sdk.language": language,
        "host.name": host,
        "deployment.environment": env,
    })


def setup(env: str = "rca-demo") -> None:
    """
    Initialize OTel providers. Call once at startup.
    Respects OTEL_COLLECTOR_ENDPOINT (default: http://localhost:4318) and
    USE_CONSOLE_EXPORTER=true for local debugging.
    """
    global _meter_provider, _meter

    use_console = os.getenv("USE_CONSOLE_EXPORTER", "").lower() == "true"
    endpoint = os.getenv("OTEL_COLLECTOR_ENDPOINT", "http://localhost:4318")

    # Metrics provider (shared)
    if use_console:
        metric_exporter = ConsoleMetricExporter()
    else:
        metric_exporter = OTLPMetricExporter(endpoint=f"{endpoint}/v1/metrics")

    reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=10_000)
    _meter_provider = MeterProvider(
        resource=Resource.create({"service.name": "rca-simulator"}),
        metric_readers=[reader],
    )
    metrics.set_meter_provider(_meter_provider)
    _meter = metrics.get_meter("rca_simulator")

    # Store env for tracer creation
    setup._env = env
    setup._use_console = use_console
    setup._endpoint = endpoint


setup._env = "rca-demo"
setup._use_console = False
setup._endpoint = "http://localhost:4318"


def get_tracer(service_name: str, language: str = "unknown", version: str = "1.0.0", host: str = "") -> trace.Tracer:
    """Get (or create) a tracer for the given service."""
    if service_name not in _tracers:
        resource = _make_resource(
            service_name, language, version,
            host or f"{service_name}-1",
            setup._env,
        )
        if setup._use_console:
            exporter = ConsoleSpanExporter()
        else:
            exporter = OTLPSpanExporter(endpoint=f"{setup._endpoint}/v1/traces")

        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        _providers[service_name] = provider
        _tracers[service_name] = provider.get_tracer(service_name)

    return _tracers[service_name]


def flush_all() -> None:
    for provider in _providers.values():
        try:
            provider.force_flush(timeout_millis=5000)
        except Exception:
            pass
    if _meter_provider:
        try:
            _meter_provider.force_flush(timeout_millis=5000)
        except Exception:
            pass


@contextmanager
def start_span(
    tracer: trace.Tracer,
    name: str,
    parent_ctx=None,
    kind: SpanKind = SpanKind.SERVER,
    attrs: Optional[dict] = None,
    duration_ms: int = 50,
    error: bool = False,
    error_msg: str = "Internal error",
    jitter_ms: int = 10,
):
    """
    Simulate a span with a fixed duration + optional jitter.
    Yields the span so callers can add child spans inside the context.

    When a non-zero time offset is active (set via set_time_offset), the span
    is stamped in the past and emitted instantly with no real sleep — used by
    the baseline seeder to bootstrap detectors without a real warmup period.
    """
    actual_duration = max(1, duration_ms + random.randint(-jitter_ms, jitter_ms))
    ctx = parent_ctx
    offset = get_time_offset()

    if offset > 0:
        # Backfill mode: past timestamps, no real sleep.
        start_ns = int((time.time() - offset) * 1e9)
        end_ns = start_ns + int(actual_duration * 1e6)
        span = tracer.start_span(name, context=ctx, kind=kind, start_time=start_ns)
        token = otel_context.attach(trace.set_span_in_context(span))
        try:
            if attrs:
                for k, v in attrs.items():
                    span.set_attribute(k, v)
            if error:
                span.set_status(StatusCode.ERROR, error_msg)
                span.record_exception(Exception(error_msg))
            yield span
        finally:
            span.end(end_time=end_ns)
            otel_context.detach(token)
    else:
        # Real-time mode: original behaviour with sleep-based duration.
        with tracer.start_as_current_span(name, context=ctx, kind=kind) as span:
            if attrs:
                for k, v in attrs.items():
                    span.set_attribute(k, v)
            if error:
                span.set_status(StatusCode.ERROR, error_msg)
                span.record_exception(Exception(error_msg))

            start = time.time()
            yield span
            elapsed_ms = (time.time() - start) * 1000
            remaining = actual_duration - elapsed_ms
            if remaining > 0:
                time.sleep(remaining / 1000)


def http_attrs(method: str, route: str, status: int, peer: str = "") -> dict:
    a = {
        "http.method": method,
        "http.route": route,
        "http.status_code": status,
        "http.scheme": "https",
    }
    if peer:
        a["net.peer.name"] = peer
    return a


def db_attrs(system: str, op: str, table: str, statement: str = "") -> dict:
    a = {
        "db.system": system,
        "db.operation": op,
        "db.sql.table": table,
    }
    if statement:
        a["db.statement"] = statement
    return a


def rpc_attrs(service: str, method: str) -> dict:
    return {
        "rpc.system": "grpc",
        "rpc.service": service,
        "rpc.method": method,
    }
