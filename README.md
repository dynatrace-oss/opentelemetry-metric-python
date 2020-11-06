# opentelemetry-metric-python

## Usage

First create the exporter:
```python
ingest_url = :INGEST_URL:
api_token = :API_TOKEN:
exporter = DynatraceMetricsExporter(ingest_url, api_token)
```
When sending data to a local OneAgent the `api_token` can be omitted.

Then register the exporter in the opentelemetry metrics pipeline:
```python
# start_pipeline will notify the MeterProvider to begin collecting/exporting
# metrics with the given meter, exporter and interval in seconds
metrics.get_meter_provider().start_pipeline(meter, exporter, 5)
``` 