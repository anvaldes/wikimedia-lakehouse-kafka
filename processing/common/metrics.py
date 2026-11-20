LATENCY_BUCKETS_MS = [100, 500, 1000, 5000, 10000, 30000, 60000, 300000, 900000, 3600000]


class Metrics:
    """Best-effort OpenTelemetry -> Cloud Monitoring. If the exporter can't load, the job runs without metrics."""

    def __init__(self, service, **attributes):
        self.attributes = attributes
        self.instruments = {}
        self.provider, self.meter = self._start(service)

    def _start(self, service):
        try:
            from shared.telemetry import init_metrics
            return init_metrics(service)
        except Exception as e:
            print(f"⚠ Telemetry disabled: {type(e).__name__}: {e}")
            return None, None

    def _get(self, name, factory, **kwargs):
        if name not in self.instruments:
            self.instruments[name] = factory(name, **kwargs)
        return self.instruments[name]

    def add(self, name, value):
        if self.meter:
            self._get(name, self.meter.create_counter).add(value, self.attributes)

    def record_ms(self, name, value):
        if self.meter:
            histogram = self._get(name, self.meter.create_histogram, unit="ms",
                                  explicit_bucket_boundaries_advisory=LATENCY_BUCKETS_MS)
            histogram.record(value, self.attributes)

    def close(self):
        """The process exits right after, so metrics must be pushed now."""
        try:
            if self.provider:
                self.provider.force_flush()
                self.provider.shutdown()
        except Exception as e:
            print(f"⚠ Could not flush metrics: {type(e).__name__}: {e}")
