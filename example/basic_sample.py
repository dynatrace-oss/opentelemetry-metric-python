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

import time

from dynatrace.opentelemetry.metric.exporter import DynatraceMetricsExporter
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider

metrics.set_meter_provider(MeterProvider())
meter = metrics.get_meter(__name__)

endpoint_url = None
api_token = None
exporter = DynatraceMetricsExporter(endpoint_url, api_token)

# start_pipeline will notify the MeterProvider to begin collecting/exporting
# metrics with the given meter, exporter and interval in seconds
metrics.get_meter_provider().start_pipeline(meter, exporter, 5)

requests_counter = meter.create_counter(
    name="requests",
    description="number of requests",
    unit="1",
    value_type=int
)

requests_size = meter.create_valuerecorder(
    name="request_size",
    description="size of requests",
    unit="1",
    value_type=int,
)


# Labels are used to identify key-values that are associated with a specific
# metric that you want to record. These are useful for pre-aggregation and can
# be used to store custom dimensions pertaining to a metric
staging_labels = {"environment": "staging"}
testing_labels = {"environment": "testing"}

# Update the metric instruments using the direct calling convention
requests_counter.add(25, staging_labels)
requests_size.record(100, staging_labels)
time.sleep(10)

requests_counter.add(50, staging_labels)
requests_size.record(5000, staging_labels)
time.sleep(5)

requests_counter.add(35, testing_labels)
requests_size.record(2, testing_labels)

input("...\n")
