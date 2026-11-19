from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.cloud_monitoring import CloudMonitoringMetricsExporter


def init_metrics(service_name, interval_ms=60000):
    reader = PeriodicExportingMetricReader(CloudMonitoringMetricsExporter(), export_interval_millis=interval_ms)
    provider = MeterProvider(metric_readers=[reader], resource=Resource.create({"service.name": service_name}))
    metrics.set_meter_provider(provider)
    return provider, metrics.get_meter(service_name)