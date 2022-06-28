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

import opentelemetry.sdk.metrics as metrics
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    PeriodicExportingMetricReader
)

from dynatrace.opentelemetry.metrics.export._exporter import (
    _DynatraceMetricsExporter
)

VERSION = "0.3.0-rc1"

_DYNATRACE_TEMPORALITY_PREFERENCE = {
    metrics.Counter: AggregationTemporality.DELTA,
    metrics.UpDownCounter: AggregationTemporality.CUMULATIVE,
    metrics.Histogram: AggregationTemporality.DELTA,
    metrics.ObservableCounter: AggregationTemporality.DELTA,
    metrics.ObservableUpDownCounter: AggregationTemporality.CUMULATIVE,
    metrics.ObservableGauge: AggregationTemporality.CUMULATIVE,
}


def configure_dynatrace_exporter(
        endpoint_url: Optional[str] = None,
        api_token: Optional[str] = None,
        prefix: Optional[str] = None,
        default_dimensions: Optional[Mapping[str, str]] = None,
        export_dynatrace_metadata: Optional[bool] = False,
        export_interval_millis: Optional[float] = None
):
    return PeriodicExportingMetricReader(
        export_interval_millis=export_interval_millis,
        preferred_temporality=_DYNATRACE_TEMPORALITY_PREFERENCE,
        exporter=_DynatraceMetricsExporter(
            endpoint_url=endpoint_url,
            api_token=api_token,
            prefix=prefix,
            default_dimensions=default_dimensions,
            export_dynatrace_metadata=export_dynatrace_metadata
        )
    )
