# Dynatrace OpenTelemetry Metrics Exporter for Python

This exporter allows exporting metrics created using the [OpenTelemetry SDK for Python](https://github.com/open-telemetry/opentelemetry-python)
directly to [Dynatrace](https://www.dynatrace.com).

**It was built against OpenTelemetry SDK version `1.12.0` and should work with any `1.12+` version.**

More information on exporting OpenTelemetry metrics to Dynatrace can be found in the
[Dynatrace documentation](https://www.dynatrace.com/support/help/shortlink/opentelemetry-metrics).

## Getting started

### Installation

To install the [latest version from PyPI](https://pypi.org/project/opentelemetry-exporter-dynatrace-metrics/) run:

```shell
pip install opentelemetry-exporter-dynatrace-metrics
```

### Usage

The general setup of OpenTelemetry Python is explained in the official [Getting Started Guide](https://open-telemetry.github.io/opentelemetry-python/getting-started.html#add-metrics).

```python
# setup metrics export pipeline
metrics.set_meter_provider(MeterProvider(
    # configure Exporter/MetricReader combination with a 5000ms export
    # interval, endpoint url and API token.
    metric_readers=[
        configure_dynatrace_metrics_export(
            export_interval_millis=5000,
            endpoint_url=endpoint_url,
            api_token=api_token)
    ]))

# get a meter
meter = metrics.get_meter(__name__)

# create a counter instrument and provide the first data point
counter = meter.create_counter(
    name="MyCounter",
    description="Description of MyCounter",
    unit="1",
    value_type=int
)

counter.add(25, {"dimension-1": "value-1"})
```

### Example

To run the [example](example/basic_example.py), clone this repository and change to the `opentelemetry-metric-python` folder, then run:

```shell
pip install .           # install the Dynatrace exporter
export LOGLEVEL=DEBUG   # (optional) Set the log level to debug to see more output (default is INFO)
python example/basic_example.py
```

A more complete setup routine can be found [here](example/install_and_run.sh), including installing inside a virtual environment and getting required packages.
If you just want to see it in action, it should be sufficient to run [`example/install_and_run.sh`](example/install_and_run.sh) from the root folder.
This script will install Python, set up a virtual environment, pull in all the required packages and run the [example](example/basic_example.py).

The example also offers a simple CLI. Run `python example/basic_example.py -h` to get more information.

### Configuration

The exporter allows for configuring the following settings by passing them to the constructor:

#### Dynatrace API Endpoint

The endpoint to which the metrics are sent is specified using the `endpoint_url` parameter.

Given an environment ID `myenv123` on Dynatrace SaaS, the [metrics ingest endpoint](https://www.dynatrace.com/support/help/dynatrace-api/environment-api/metric-v2/post-ingest-metrics/) would be `https://myenv123.live.dynatrace.com/api/v2/metrics/ingest`.

If a OneAgent is installed on the host, it can provide a local endpoint for providing metrics directly without the need for an API token.
This feature is currently in an Early Adopter phase and has to be enabled as described in the [OneAgent metric API documentation](https://www.dynatrace.com/support/help/how-to-use-dynatrace/metrics/metric-ingestion/ingestion-methods/local-api/).
Using the local API endpoint, the host ID and host name context are automatically added to each metric as dimensions.
The default metric API endpoint exposed by the OneAgent is `http://localhost:14499/metrics/ingest`.
If no endpoint is set and a OneAgent is running on the host, metrics will be exported to it automatically using the OneAgent with no endpoint or API token configuration required.

#### Dynatrace API Token

The Dynatrace API token to be used by the exporter is specified using the `api_token` parameter and could, for example, be read from an environment variable.

Creating an API token for your Dynatrace environment is described in the [Dynatrace API documentation](https://www.dynatrace.com/support/help/dynatrace-api/basics/dynatrace-api-authentication/).
The permission required for sending metrics is `Ingest metrics` (`metrics.ingest`) and it is recommended to limit scope to only this permission.

#### Metric Key Prefix

The `prefix` parameter specifies an optional prefix, which is prepended to each metric key, separated by a dot (`<prefix>.<name>`).

#### Default Dimensions

The `default_dimensions` parameter can be used to optionally specify a list of key/value pairs, which will be added as additional dimensions to all data points.
Dimension keys are unique, and labels on instruments will overwrite the default dimensions if key collisions appear.

#### Export Dynatrace Metadata

If running on a host with a running OneAgent, setting the `export_dynatrace_metadata` option to `True` will export metadata collected by the OneAgent to the Dynatrace endpoint.
If no Dynatrace API endpoint is set, the default exporter endpoint will be the OneAgent endpoint, and this option will be set automatically.
Therefore, if no endpoint is specified, a OneAgent is assumed to be running and used as the export endpoint for all metric lines, including metadata.
More information on the underlying Dynatrace metadata feature that is used by the exporter can be found in the
[Dynatrace documentation](https://www.dynatrace.com/support/help/how-to-use-dynatrace/metrics/metric-ingestion/ingestion-methods/enrich-metrics/).

##### Dimensions precedence

When specifying default dimensions, attributes and Dynatrace metadata enrichment, the precedence of dimensions with the same key is as follows:
Default dimensions are overwritten by attributes passed to instruments, which in turn are overwritten by the Dynatrace metadata dimensions (even though the likeliness of a collision here is very low, since the Dynatrace metadata only contains [Dynatrace reserved dimensions](https://www.dynatrace.com/support/help/how-to-use-dynatrace/metrics/metric-ingestion/metric-ingestion-protocol/#syntax) starting with `dt.*`).

## Development

### Requirements

Just [`tox`](https://pypi.org/project/tox/).

### Running tests and lint

* Test all supported python versions: `tox`
* Test all supported python versions in parallel: `tox -p`
* A particular python version: `tox -e 38`
* Current python version: `tox -e py`
* Lint: `tox -e lint`

## Limitations

### Histogram

OpenTelemetry Histograms are exported to Dynatrace as statistical summaries
consisting of a minimum and maximum value, the total sum of all values, and the
count of the values summarized. If the min and max values are not directly
available on the metric data point, estimations based on the boundaries of the
first and last buckets containing values are used.