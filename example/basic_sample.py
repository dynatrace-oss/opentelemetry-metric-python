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

from dynatrace.opentelemetry.metrics.export import DynatraceMetricsExporter
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from os.path import splitext, basename
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Example exporting metrics using the Dynatrace metrics "
                    "exporter.",
        epilog="The script can be run without any arguments. In that case, "
               "the local OneAgent is used as an endpoint, if it is installed.")
    parser.add_argument("-e", "--endpoint", type=str, dest="endpoint",
                        default="http://127.0.0.1:14499/metrics/ingest",
                        help="The endpoint url used to export metrics to. "
                             "This can be either a Dynatrace metrics "
                             "ingestion endpoint, or a local OneAgent "
                             "endpoint. If no value is set, use the "
                             "local OneAgent endpoint.")

    parser.add_argument("-t", "--token", default=None, type=str, dest="token",
                        help="API Token generated in the Dynatrace UI. Needs "
                             "to have metrics ingestion enabled in order to "
                             "work correctly. Can be omitted when exporting "
                             "to the local OneAgent.")

    parser.add_argument("-nm", "--no-metadata", dest="metadata_enrichment",
                        action="store_false",
                        help="Turn off OneAgent Metadata enrichment. If no "
                             "OneAgent is running on the machine, this is "
                             "ignored. Otherwise, OneAgent metadata will be "
                             "added to each of the exported metric lines.")

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

    # set up opentelemetry for export:
    metrics.set_meter_provider(MeterProvider())
    meter = metrics.get_meter(splitext(basename(__file__))[0])

    exporter = DynatraceMetricsExporter(args.endpoint, args.token,
                                        prefix="otel.python",
                                        export_oneagent_metadata=args.metadata_enrichment)

    # This call registers the meter and exporter with the global
    # MeterProvider set above. All instruments created by the meter that is
    # registered here will export to the Dynatrace metrics exporter. It is a
    # good idea to keep a reference to the meter (e. g. in a global variable)
    # in order to create instruments anywhere in the code that all export to
    # the same Dynatrace metrics exporter.
    metrics.get_meter_provider().start_pipeline(meter, exporter, args.interval)

    requests_counter = meter.create_counter(
        name="requests",
        description="number of requests",
        unit="1",
        value_type=int
    )

    requests_size = meter.create_valuerecorder(
        name="request_size_bytes",
        description="size of requests",
        unit="byte",
        value_type=int,
    )
    #
    # Labels are used to identify key-values that are associated with a specific
    # metric that you want to record. These are useful for pre-aggregation and
    # can be used to store custom dimensions pertaining to a metric
    staging_labels = {"environment": "staging"}
    testing_labels = {"environment": "testing"}

    try:
        while True:
            # Update the metric instruments using the direct calling convention
            requests_counter.add(25, staging_labels)
            requests_size.record(100, staging_labels)
            time.sleep(5)

            requests_counter.add(50, staging_labels)
            requests_size.record(5000, staging_labels)
            time.sleep(5)

            requests_counter.add(35, testing_labels)
            requests_size.record(2, testing_labels)
            time.sleep(5)

    except KeyboardInterrupt:
        print("shutting down...")
