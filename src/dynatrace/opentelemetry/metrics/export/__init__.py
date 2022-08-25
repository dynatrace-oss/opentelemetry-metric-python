# Copyright 2020 Dynatrace LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Mapping, Optional

from dynatrace.opentelemetry.metrics.export._exporter import (
    _DynatraceMetricsExporter,
)
from opentelemetry.sdk.metrics.export import (
    PeriodicExportingMetricReader,
    MetricReader,
)

VERSION = "0.3.0"


def configure_dynatrace_metrics_export(
    endpoint_url: Optional[str] = None,
    api_token: Optional[str] = None,
    prefix: Optional[str] = None,
    default_dimensions: Optional[Mapping[str, str]] = None,
    export_dynatrace_metadata: Optional[bool] = False,
    export_interval_millis: Optional[float] = None
) -> MetricReader:
    """
    Configures and creates a PeriodicExportingMetricReader and
    DynatraceMetricsExporter combination.

    Parameters
    ----------
    endpoint_url: str, Optional
        The endpoint to send metrics to. Given an environment ID `myenv123` on
        Dynatrace SaaS, the endpoint_url would be
        `https://myenv123.live.dynatrace.com/api/v2/metrics/ingest`.
        (default: local OneAgent Endpoint)
    api_token: str, Optional
        The API token for your Dynatrace environment with at least the scope
        `metrics.ingest`.
        (default: no API token).
    prefix: str, Optional
        Will be prepended to each metric key, separated by a dot
        (`<prefix>.<name>`).
        (default: no prefix)
    default_dimensions: Mapping[str, str], Optional
        Static dimensions to add to every metric. Dimension keys need
        to be unique, attributes on instruments will overwrite the default
        dimensions if key collisions appear.
        (default: empty)
    export_dynatrace_metadata: bool, Optional
        If running on a host with a running OneAgent,
        setting the `export_dynatrace_metadata` option to `True` will export
        metadata collected by the OneAgent to the Dynatrace endpoint. This
        option will default to `True` when `endpoint_url` is not set.
        (default: `False`)
    export_interval_millis: float, Optional
        Time to wait between exports in milliseconds.
        (default: `60000`)
    Returns
    -------
    PeriodicExportingMetricReader, configured with a Dynatrace metrics exporter
    according to this method's parameters.
    """
    return PeriodicExportingMetricReader(
        export_interval_millis=export_interval_millis,
        exporter=_DynatraceMetricsExporter(
            endpoint_url=endpoint_url,
            api_token=api_token,
            prefix=prefix,
            default_dimensions=default_dimensions,
            export_dynatrace_metadata=export_dynatrace_metadata,
        )
    )
