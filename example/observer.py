import psutil

from dynatrace.opentelemetry.metric.exporter import DynatraceMetricsExporter
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider

endpoint_url = None
auth_token = None
exporter = DynatraceMetricsExporter(endpoint_url, auth_token)

metrics.set_meter_provider(MeterProvider())
meter = metrics.get_meter(__name__)
metrics.get_meter_provider().start_pipeline(meter, exporter, 5)


# Callback to gather cpu usage
def get_cpu_usage_callback(observer):
    for (number, percent) in enumerate(psutil.cpu_percent(percpu=True)):
        labels = {"cpu_number": str(number)}
        observer.observe(percent, labels)


meter.register_valueobserver(
    callback=get_cpu_usage_callback,
    name="cpu_percent",
    description="per-cpu usage",
    unit="1",
    value_type=float,
)


# Callback to gather RAM memory usage
def get_ram_usage_callback(observer):
    ram_percent = psutil.virtual_memory().percent
    observer.observe(ram_percent, {})


meter.register_valueobserver(
    callback=get_ram_usage_callback,
    name="ram_percent",
    description="RAM memory usage",
    unit="1",
    value_type=float,
)

input("Metrics will be printed soon. Press a key to finish...\n")
