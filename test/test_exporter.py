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

import unittest
from unittest.mock import patch

import requests
from opentelemetry.sdk._metrics.export import Metric, \
    MetricExportResult
from opentelemetry.sdk._metrics.point import PointT, Gauge, Sum, AggregationTemporality
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.util.instrumentation import InstrumentationInfo

from dynatrace.opentelemetry.metrics.export import DynatraceMetricsExporter


class TestExporterCreation(unittest.TestCase):

    def setUp(self) -> None:
        self._instrument_name = "my.instr"
        self._attributes = (("l1", "v1"), ("l2", "v2"))
        self._headers = {
            "Accept": "*/*; q=0",
            "Content-Type": "text/plain; charset=utf-8",
            "User-Agent": "opentelemetry-metric-python",
        }
        # 01/01/2021 00:00:00
        self._test_timestamp_nanos = 1609455600000000000
        self._test_timestamp_millis = int(self._test_timestamp_nanos / 1000000)

        self._ingest_endpoint = "http://localhost:14499/metrics/ingest"

    @patch.object(requests.Session, 'post')
    def test_empty_records(self, mock_post):
        mock_post.return_value = self._get_session_response()

        exporter = DynatraceMetricsExporter()
        result = exporter.export([])
        self.assertEqual(MetricExportResult.SUCCESS, result)

        mock_post.assert_not_called()

    @patch.object(requests.Session, 'post')
    def test_all_optional(self, mock_post):
        mock_post.return_value = self._get_session_response()

        metric = self._create_record(self._create_sum_data_point(10))

        exporter = DynatraceMetricsExporter()
        exporter._is_delta_export = True
        result = exporter.export([metric])

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 "
                 + str(self._test_timestamp_millis)
                 + "\n",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_endpoint(self, mock_post):
        mock_post.return_value = self._get_session_response()

        endpoint = "https://abc1234.dynatrace.com/metrics/ingest"

        metric = self._create_record(self._create_sum_data_point(10))

        exporter = DynatraceMetricsExporter(endpoint_url=endpoint)
        exporter._is_delta_export = True
        result = exporter.export([metric])

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 "
                 + str(self._test_timestamp_millis)
                 + "\n",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_endpoint_and_token(self, mock_post):
        mock_post.return_value = self._get_session_response()

        endpoint = "https://abc1234.dynatrace.com/metrics/ingest"
        token = "my.secret.token"
        # add the token to the expected headers
        self._headers["Authorization"] = "Api-Token {}".format(token)

        metric = self._create_record(self._create_sum_data_point(10))

        exporter = DynatraceMetricsExporter(endpoint_url=endpoint, api_token=token)
        exporter._is_delta_export = True
        result = exporter.export([metric])

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 "
                 + str(self._test_timestamp_millis)
                 + "\n",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_only_token(self, mock_post):
        mock_post.return_value = self._get_session_response()

        # token is not added in the expected headers
        token = "my.secret.token"

        metric = self._create_record(self._create_sum_data_point(10))

        exporter = DynatraceMetricsExporter(api_token=token)
        exporter._is_delta_export = True
        result = exporter.export([metric])

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 "
                 + str(self._test_timestamp_millis)
                 + "\n",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_prefix(self, mock_post):
        mock_post.return_value = self._get_session_response()

        metric = self._create_record(self._create_sum_data_point(10))

        prefix = "test_prefix"
        exporter = DynatraceMetricsExporter(prefix=prefix)
        exporter._is_delta_export = True
        result = exporter.export([metric])

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="{0}.my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 {1}\n".format(prefix,
                                                                                                        self._test_timestamp_millis),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_tags(self, mock_post):
        mock_post.return_value = self._get_session_response()

        metric = self._create_record(self._create_sum_data_point(10))

        tags = {"tag1": "tv1", "tag2": "tv2"}
        exporter = DynatraceMetricsExporter(default_dimensions=tags)
        exporter._is_delta_export = True
        result = exporter.export([metric])

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,tag1=tv1,tag2=tv2,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 "
                 + str(self._test_timestamp_millis)
                 + "\n",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    @patch('dynatrace.metric.utils._dynatrace_metadata_enricher'
           '.DynatraceMetadataEnricher._get_metadata_file_content')
    def test_dynatrace_metadata_enrichment_with_default_tags(
            self, mock_enricher, mock_post):
        mock_post.return_value = self._get_session_response()

        # tags coming from the Dynatrace metadata enricher
        mock_enricher.return_value = [
            "dt_mtag1=value1",
            "dt_mtag2=value2"
        ]

        default_tags = {"tag1": "tv1", "tag2": "tv2"}

        metric = self._create_record(self._create_sum_data_point(10))

        exporter = DynatraceMetricsExporter(
            default_dimensions=default_tags,
            export_dynatrace_metadata=True)
        exporter._is_delta_export = True
        result = exporter.export([metric])

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,tag1=tv1,tag2=tv2,l1=v1,l2=v2,"
                 "dt_mtag1=value1,dt_mtag2=value2,"
                 "dt.metrics.source=opentelemetry count,delta=10 "
                 + str(self._test_timestamp_millis)
                 + "\n",
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    @patch('dynatrace.metric.utils.dynatrace_metrics_api_constants'
           '.DynatraceMetricsApiConstants.payload_lines_limit')
    def test_invalid_metricname_skipped(self, mock_const, mock_post):
        mock_post.return_value = self._get_session_response()
        mock_const.return_value = 2

        metrics = []
        for n in range(4):
            data_point = self._create_sum_data_point(n)
            if n == 3:
                # for the last metric, we create one with an invalid name
                metric = Metric(attributes=self._attributes,
                                name="",
                                point=data_point,
                                description="",
                                unit="1",
                                resource=Resource({}),
                                instrumentation_info=InstrumentationInfo(name="dynatrace.opentelemetry.metrics.export",
                                                                         version="0.0.1"))
                metrics.append(metric)
            else:
                metrics.append(self._create_record(data_point))

        first_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=0 {0}\nmy.instr,l1=v1," \
                         "l2=v2,dt.metrics.source=opentelemetry count,delta=1 {0}\n".format(self._test_timestamp_millis)

        # the second export misses the metric with delta=3 because the name was invalid
        second_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=2 {0}\n" \
            .format(self._test_timestamp_millis)

        exporter = DynatraceMetricsExporter()
        exporter._is_delta_export = True
        result = exporter.export(metrics)

        self.assertEqual(MetricExportResult.SUCCESS, result)

        mock_post.assert_any_call(
            self._ingest_endpoint,
            data=second_expected,
            headers=self._headers)

        mock_post.assert_any_call(
            self._ingest_endpoint,
            data=first_expected,
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    @patch('dynatrace.metric.utils.dynatrace_metrics_api_constants'
           '.DynatraceMetricsApiConstants.payload_lines_limit')
    def test_fail_to_serialize_skipped(self, mock_const, mock_post):
        mock_post.return_value = self._get_session_response()
        mock_const.return_value = 2

        metrics = []
        timestamps = []
        for n in range(4):
            data_point = self._create_sum_data_point(n)

            if n == 3:
                # for the last metric, we create one with an invalid name
                # this will only fail when calling the serializer
                metric = Metric(attributes=self._attributes,
                                name=".",
                                point=data_point,
                                description="",
                                unit="1",
                                resource=Resource({}),
                                instrumentation_info=InstrumentationInfo(
                                    name="dynatrace.opentelemetry.metrics.export",
                                    version="0.0.1"))
                timestamps.append(int(data_point.time_unix_nano / 1000000))
                metrics.append(metric)
            else:
                metrics.append(self._create_record(data_point))
                timestamps.append(int(data_point.time_unix_nano / 1000000))

        first_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=0 {0}\nmy.instr,l1=v1," \
                         "l2=v2,dt.metrics.source=opentelemetry count,delta=1 {0}\n".format(self._test_timestamp_millis)

        # the second export misses the metric with delta=3 because the name was invalid
        second_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=2 {0}\n".format(
            str(self._test_timestamp_millis))

        exporter = DynatraceMetricsExporter()
        exporter._is_delta_export = True
        result = exporter.export(metrics)

        self.assertEqual(MetricExportResult.SUCCESS, result)

        mock_post.assert_any_call(
            self._ingest_endpoint,
            data=second_expected,
            headers=self._headers)

        mock_post.assert_any_call(
            self._ingest_endpoint,
            data=first_expected,
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    @patch('dynatrace.metric.utils.dynatrace_metrics_api_constants'
           '.DynatraceMetricsApiConstants.payload_lines_limit')
    def test_entire_batch_fail(self, mock_const, mock_post):
        mock_post.side_effect = [self._get_session_response(),
                                 self._get_session_response(error=True)]
        mock_const.return_value = 2

        metrics = []
        for n in range(4):
            metrics.append(self._create_record(self._create_sum_data_point(n)))

        first_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=0 {0}\n" \
                         "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=1 {0}\n".format(
            self._test_timestamp_millis)
        second_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=2 {0}\n" \
                          "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=3 {0}\n".format(
            self._test_timestamp_millis)

        exporter = DynatraceMetricsExporter()
        exporter._is_delta_export = True
        result = exporter.export(metrics)

        # should have failed the whole batch as the second POST request failed
        self.assertEqual(MetricExportResult.FAILURE, result)

        mock_post.assert_any_call(
            self._ingest_endpoint,
            data=first_expected,
            headers=self._headers)

        mock_post.assert_any_call(
            self._ingest_endpoint,
            data=second_expected,
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_sum_aggregator_delta(self, mock_post):
        metric = self._create_record(self._create_sum_data_point(10))

        exporter = DynatraceMetricsExporter()
        exporter._is_delta_export = True

        result = exporter.export([metric])

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 {0}\n"
                .format(self._test_timestamp_millis),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_gauge_aggregator_total_reported_as_gauge(self, mock_post):
        data_point = Gauge(value=10,
                           time_unix_nano=self._test_timestamp_nanos)
        metric = self._create_record(data_point)

        exporter = DynatraceMetricsExporter()
        exporter._is_delta_export = False
        result = exporter.export([metric])

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry gauge,10 {0}\n".format(
                str(int(self._test_timestamp_nanos / 1000000))),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_multiple_records(self, mock_post):
        mock_post.return_value = self._get_session_response()

        records = []
        records.append(self._create_record(self._create_sum_data_point(10)))
        records.append(self._create_record(Gauge(time_unix_nano=self._test_timestamp_nanos, value=20)))

        exporter = DynatraceMetricsExporter()
        exporter._is_delta_export = True
        result = exporter.export(records)

        expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 {0}\n" \
                   "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry gauge,20 {0}\n"\
            .format(int(self._test_timestamp_nanos / 1000000))

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data=expected,
            headers=self._headers)

    def _create_record(self, point: PointT):
        return Metric(attributes=self._attributes,
                      name=self._instrument_name,
                      point=point,
                      description="",
                      unit="1",
                      resource=Resource({}),
                      instrumentation_info=InstrumentationInfo(name="dynatrace.opentelemetry.metrics.export",
                                                               version="0.0.1"))

    def _create_sum_data_point(self, value: int) -> Sum:
        return Sum(start_time_unix_nano=self._test_timestamp_nanos,
                   aggregation_temporality=AggregationTemporality.DELTA,
                   is_monotonic=True,
                   time_unix_nano=self._test_timestamp_nanos,
                   value=value)

    @staticmethod
    def _get_session_response(error: bool = False) -> requests.Response:
        r = requests.Response()
        if error:
            r.status_code = 500
        else:
            r.status_code = 200
            r._content = str.encode('{}')
        return r


if __name__ == '__main__':
    unittest.main()
