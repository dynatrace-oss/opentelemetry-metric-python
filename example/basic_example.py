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

import argparse
import logging
import os
import random
import time
from os.path import splitext, basename

import psutil
from opentelemetry import _metrics
from opentelemetry._metrics.measurement import Measurement
from opentelemetry.sdk._metrics import MeterProvider
from opentelemetry.sdk._metrics.export import PeriodicExportingMetricReader

from dynatrace.opentelemetry.metrics.export import DynatraceMetricsExporter


# Callback to gather cpu usage
def get_cpu_usage_callback():
    for (number, percent) in enumerate(psutil.cpu_percent(percpu=True)):
        attributes = {"cpu_number": str(number)}
        yield Measurement(percent, attributes)


# Callback to gather RAM memory usage
def get_ram_usage_callback():
    ram_percent = psutil.virtual_memory().percent
    yield Measurement(ram_percent)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Example exporting metrics using the Dynatrace metrics "
                    "exporter.",
        epilog="The script can be run without any arguments. In that case, the"
               " local OneAgent is used as an endpoint, if it is installed.")
    parser.add_argument("-e", "--endpoint", default=None, type=str,
                        dest="endpoint",
                        help="The endpoint url used to export metrics to. "
                             "This can be either a Dynatrace metrics "
                             "ingestion endpoint, or a local OneAgent "
                             "endpoint. If no value is set, the default "
                             "local OneAgent endpoint is used.")

    parser.add_argument("-t", "--token", default=None, type=str, dest="token",
                        help="API Token generated in the Dynatrace UI. Needs "
                             "to have metrics ingestion enabled in order to "
                             "work correctly. Can be omitted when exporting "
                             "to the local OneAgent.")

    parser.add_argument("-nm", "--no-metadata", dest="metadata_enrichment",
                        action="store_false",
                        help="Turn off Dynatrace Metadata enrichment. If no "
                             "OneAgent is running on "
                             "the host, this is ignored. Otherwise, Dynatrace "
                             "metadata will be added to each of the exported "
                             "metric lines.")

    parser.add_argument("-i", "--interval", default=10., type=float,
                        dest="interval",
                        help="Set the export interval in seconds for the "
                             "Dynatrace metrics exporter. This specifies how "
                             "often data is exported to Dynatrace. We suggest "
                             "using export intervals of 10 to 60 seconds. The "
                             "default interval is 10 seconds.")

    parser.set_defaults(metadata_enrichment=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()

    script_name = splitext(basename(__file__))[0]

    # try to read the log level from the environment variable "LOGLEVEL" and
    # setting it to "INFO" if not found.
    # Valid levels are: DEBUG, INFO, WARN/WARNING, ERROR, CRITICAL/FATAL
    loglevel = os.environ.get("LOGLEVEL", "INFO").upper()
    logging.basicConfig(level=loglevel)
    logger = logging.getLogger(script_name)

    if not args.endpoint:
        logger.info(
            "No Dynatrace endpoint specified, exporting to default local "
            "OneAgent endpoint.")

    # set up OpenTelemetry for export:
    # This call sets up the MeterProvider, with a PeriodicExportingMetricReader that exports every 5000 ms
    # and the Dynatrace exporter exporting to args.endpoint with args.token
    logger.debug("setting up global OpenTelemetry configuration.")
    _metrics.set_meter_provider(MeterProvider(
        metric_readers=[PeriodicExportingMetricReader(
            export_interval_millis=5000,
            exporter=DynatraceMetricsExporter(args.endpoint, args.token,
                                              prefix="otel.python",
                                              export_dynatrace_metadata=args.metadata_enrichment,
                                              default_dimensions={"default1": "defval1"}))]))

    meter = _metrics.get_meter(splitext(basename(__file__))[0])

    logger.info("creating instruments to record metrics data")
    requests_counter = meter.create_counter(
        name="requests",
        description="number of requests",
        unit="1"
    )

    requests_size = meter.create_histogram(
        name="request_size_bytes",
        description="size of requests",
        unit="byte"
    )

    meter.create_observable_gauge(
        callback=get_cpu_usage_callback,
        name="cpu_percent",
        description="per-cpu usage",
        unit="1"
    )

    meter.create_observable_gauge(
        callback=get_ram_usage_callback,
        name="ram_percent",
        description="RAM memory usage",
        unit="1",
    )

    # Attributes are used to identify key-values that are associated with a
    # specific metric that you want to record. These are useful for
    # pre-aggregation and can be used to store custom dimensions pertaining
    # to a metric
    staging_attributes = {"environment": "staging"}
    testing_attributes = {"environment": "testing"}

    logger.info("starting instrumented application...")
    try:
        while True:
            # Update the metric instruments using the direct calling convention
            requests_counter.add(random.randint(0, 25), staging_attributes)
            requests_size.record(random.randint(0, 300), staging_attributes)

            requests_counter.add(random.randint(0, 35), testing_attributes)
            requests_size.record(random.randint(0, 100), testing_attributes)
            time.sleep(5)

    except KeyboardInterrupt:
        logger.info("shutting down...")
