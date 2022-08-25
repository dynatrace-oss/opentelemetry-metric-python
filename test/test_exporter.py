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

import math
import re
import unittest
from typing import Sequence, Union
from unittest import mock
from unittest.mock import patch

import requests
from dynatrace.opentelemetry.metrics.export import (
    _DynatraceMetricsExporter,
    configure_dynatrace_metrics_export,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    Metric,
    MetricExportResult,
    PeriodicExportingMetricReader,
    Gauge,
    Sum,
    AggregationTemporality,
    Histogram, MetricsData,
    NumberDataPoint,
    DataT,
    ResourceMetrics,
    ScopeMetrics,
    HistogramDataPoint,
)
from opentelemetry.sdk.metrics.view import View
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from parameterized import parameterized


class AnyStringMatching(str):
    def __eq__(self, other):
        return re.match(str(self), other)


class TestExporter(unittest.TestCase):

    def setUp(self) -> None:
        self._instrument_name = "my.instr"
        self._attributes = {
            "l1": "v1",
            "l2": "v2"
        }
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

        exporter = _DynatraceMetricsExporter()
        metrics_data = MetricsData(resource_metrics=[])
        result = exporter.export(metrics_data)
        self.assertEqual(MetricExportResult.SUCCESS, result)

        mock_post.assert_not_called()

    @patch.object(requests.Session, 'post')
    def test_all_optional(self, mock_post):
        mock_post.return_value = self._get_session_response()

        metrics_data = self._metrics_data_from_data([self._create_sum(10)])

        exporter = _DynatraceMetricsExporter()
        result = exporter.export(metrics_data)

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 "
                 + str(self._test_timestamp_millis),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_endpoint(self, mock_post):
        mock_post.return_value = self._get_session_response()

        endpoint = "https://abc1234.dynatrace.com/metrics/ingest"

        metrics_data = self._metrics_data_from_data([self._create_sum(10)])

        exporter = _DynatraceMetricsExporter(endpoint_url=endpoint)
        result = exporter.export(metrics_data)

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 "
                 + str(self._test_timestamp_millis),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_endpoint_and_token(self, mock_post):
        mock_post.return_value = self._get_session_response()

        endpoint = "https://abc1234.dynatrace.com/metrics/ingest"
        token = "my.secret.token"
        # add the token to the expected headers
        self._headers["Authorization"] = "Api-Token {}".format(token)

        metrics_data = self._metrics_data_from_data([self._create_sum(10)])

        exporter = _DynatraceMetricsExporter(endpoint_url=endpoint,
                                             api_token=token)
        result = exporter.export(metrics_data)

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 "
                 + str(self._test_timestamp_millis),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_only_token(self, mock_post):
        mock_post.return_value = self._get_session_response()

        # token is not added in the expected headers
        token = "my.secret.token"

        metrics_data = self._metrics_data_from_data([self._create_sum(10)])

        exporter = _DynatraceMetricsExporter(api_token=token)
        result = exporter.export(metrics_data)

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 "
                 + str(self._test_timestamp_millis),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_prefix(self, mock_post):
        mock_post.return_value = self._get_session_response()

        metrics_data = self._metrics_data_from_data([self._create_sum(10)])

        prefix = "test_prefix"
        exporter = _DynatraceMetricsExporter(prefix=prefix)
        result = exporter.export(metrics_data)

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="{0}.my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 {1}"
                .format(prefix, self._test_timestamp_millis),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_with_default_dimensions(self, mock_post):
        mock_post.return_value = self._get_session_response()

        metrics_data = self._metrics_data_from_data([self._create_sum(10)])

        dimensions = {"attribute1": "tv1", "attribute2": "tv2"}
        exporter = _DynatraceMetricsExporter(default_dimensions=dimensions)
        result = exporter.export(metrics_data)

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,attribute1=tv1,attribute2=tv2,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 "
                 + str(self._test_timestamp_millis),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    @patch('dynatrace.metric.utils._dynatrace_metadata_enricher'
           '.DynatraceMetadataEnricher._get_metadata_file_content')
    def test_dynatrace_metadata_enrichment_with_default_attributes(
            self, mock_enricher, mock_post):
        mock_post.return_value = self._get_session_response()

        # attributes coming from the Dynatrace metadata enricher
        mock_enricher.return_value = [
            "dt_mattribute1=value1",
            "dt_mattribute2=value2"
        ]

        default_attributes = {"attribute1": "tv1", "attribute2": "tv2"}

        metrics_data = self._metrics_data_from_data([self._create_sum(10)])

        exporter = _DynatraceMetricsExporter(
            default_dimensions=default_attributes,
            export_dynatrace_metadata=True)
        result = exporter.export(metrics_data)

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,attribute1=tv1,attribute2=tv2,l1=v1,l2=v2,"
                 "dt_mattribute1=value1,dt_mattribute2=value2,"
                 "dt.metrics.source=opentelemetry count,delta=10 "
                 + str(self._test_timestamp_millis),
            headers=self._headers)

    @parameterized.expand([
        ("",),
        (".",)
    ])
    def test_invalid_metricname_skipped(self, instrument_name):
        with patch.object(requests.Session, 'post') as mock_post:
            mock_post.return_value = self._get_session_response()

            metrics = []
            for n in range(4):
                data = self._create_sum(n)
                if n == 3:
                    # create metrics with invalid name.
                    metric = self._metric_from_data(data,
                                                    instrument_name=instrument_name)
                    metrics.append(metric)
                else:
                    metrics.append(self._metric_from_data(data))

            expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=0 {0}\n" \
                       "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=1 {0}\n" \
                       "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=2 {0}" \
                .format(self._test_timestamp_millis)

            exporter = _DynatraceMetricsExporter()
            result = exporter.export(
                self._metrics_data_from_metrics(metrics))

            self.assertEqual(MetricExportResult.SUCCESS, result)

            mock_post.assert_any_call(
                self._ingest_endpoint,
                data=expected,
                headers=self._headers)

    @patch.object(requests.Session, 'post')
    @patch('dynatrace.metric.utils.dynatrace_metrics_api_constants'
           '.DynatraceMetricsApiConstants.payload_lines_limit')
    def test_batching(self, mock_const, mock_post):
        mock_post.return_value = self._get_session_response()
        mock_const.return_value = 1

        metrics = [self._create_sum(n) for n in range(2)]

        first_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=0 {0}" \
            .format(self._test_timestamp_millis)
        second_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=1 {0}" \
            .format(self._test_timestamp_millis)

        exporter = _DynatraceMetricsExporter()
        result = exporter.export(
            self._metrics_data_from_data(metrics))

        # should have failed the whole batch as the second POST request failed
        self.assertEqual(MetricExportResult.SUCCESS, result)

        mock_post.assert_any_call(
            self._ingest_endpoint,
            data=first_expected,
            headers=self._headers)

        mock_post.assert_any_call(
            self._ingest_endpoint,
            data=second_expected,
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    @patch('dynatrace.metric.utils.dynatrace_metrics_api_constants'
           '.DynatraceMetricsApiConstants.payload_lines_limit')
    def test_entire_batch_fail(self, mock_const, mock_post):
        mock_post.side_effect = [self._get_session_response(),
                                 self._get_session_response(error=True)]
        mock_const.return_value = 2

        metrics = [self._create_sum(n) for n in range(4)]

        first_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=0 {0}\n" \
                         "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=1 {0}" \
            .format(self._test_timestamp_millis)
        second_expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=2 {0}\n" \
                          "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=3 {0}" \
            .format(self._test_timestamp_millis)

        exporter = _DynatraceMetricsExporter()
        result = exporter.export(
            self._metrics_data_from_data(metrics))

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
    def test_monotonic_delta_sum_exported_as_counter(self, mock_post):
        metrics_data = self._metrics_data_from_data([self._create_sum(10)])

        exporter = _DynatraceMetricsExporter()

        result = exporter.export(metrics_data)

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 {0}"
                .format(self._test_timestamp_millis),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_non_monotonic_delta_sum_is_dropped(self, mock_post):
        metrics_data = self._metrics_data_from_data([
            self._create_sum(
                10,
                monotonic=False,
                aggregation_temporality=AggregationTemporality.DELTA)])

        exporter = _DynatraceMetricsExporter()

        result = exporter.export(metrics_data)

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_not_called()

    @patch.object(requests.Session, 'post')
    def test_monotonic_cumulative_sum_is_dropped(self, mock_post):
        metrics_data = self._metrics_data_from_data([self._create_sum(10,
                                                                      monotonic=True,
                                                                      aggregation_temporality=AggregationTemporality.CUMULATIVE)])

        exporter = _DynatraceMetricsExporter()

        result = exporter.export(metrics_data)

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_not_called()

    @patch.object(requests.Session, 'post')
    def test_non_monotonic_cumulative_sum_exported_as_gauge(self, mock_post):
        metrics_data = self._metrics_data_from_data([self._create_sum(10,
                                                                      monotonic=False,
                                                                      aggregation_temporality=AggregationTemporality.CUMULATIVE)])

        exporter = _DynatraceMetricsExporter()

        result = exporter.export(metrics_data)

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry gauge,10 {0}"
                .format(str(int(self._test_timestamp_nanos / 1000000))),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_gauge_reported_as_gauge(self, mock_post):
        data = self._create_gauge(value=10)

        exporter = _DynatraceMetricsExporter()
        result = exporter.export(
            self._metrics_data_from_data([data]))

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry gauge,10 {0}"
                .format(str(int(self._test_timestamp_nanos / 1000000))),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_histogram_exported_as_gauge(self, mock_post):
        data = self._create_histogram(
            bucket_counts=[1, 2, 4, 5],
            explicit_bounds=[0, 5, 10],
            histogram_sum=87,
            histogram_min=-3,
            histogram_max=12
        )

        metrics_data = self._metrics_data_from_data([data])

        exporter = _DynatraceMetricsExporter()
        result = exporter.export(metrics_data)

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry gauge,min=-3,max=12,sum=87,count=12 {0}"
                .format(str(int(self._test_timestamp_nanos / 1000000))),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_cumulative_histogram_dropped(self, mock_post):
        data = self._create_histogram(
            bucket_counts=[1, 2, 4, 5],
            explicit_bounds=[0, 5, 10],
            aggregation_temporality=AggregationTemporality.CUMULATIVE
        )

        metrics_data = self._metrics_data_from_data([data])

        exporter = _DynatraceMetricsExporter()
        result = exporter.export(metrics_data)

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_not_called()

    @patch.object(requests.Session, 'post')
    def test_histogram_without_min_max_exported_as_estimated_gauge(self,
                                                                   mock_post):

        data = self._create_histogram(bucket_counts=[1, 2, 4, 5],
                                      explicit_bounds=[0, 5, 10],
                                      histogram_sum=87,
                                      histogram_min=math.inf,
                                      histogram_max=-math.inf
                                      )
        metrics_data = self._metrics_data_from_data([data])

        exporter = _DynatraceMetricsExporter()
        result = exporter.export(metrics_data)

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data="my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry gauge,min=0,max=10,sum=87,count=12 {0}"
                .format(str(int(self._test_timestamp_nanos / 1000000))),
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_multiple_records(self, mock_post):
        mock_post.return_value = self._get_session_response()

        data = [
            self._create_sum(10),
            self._create_gauge(value=20),
            self._create_histogram(bucket_counts=[1, 2, 4, 5],
                                   explicit_bounds=[0, 5, 10],
                                   histogram_min=-3,
                                   histogram_max=12,
                                   histogram_sum=87
                                   )
        ]
        exporter = _DynatraceMetricsExporter()
        result = exporter.export(self._metrics_data_from_data(data))

        expected = "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry count,delta=10 {0}\n" \
                   "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry gauge,20 {0}\n" \
                   "my.instr,l1=v1,l2=v2,dt.metrics.source=opentelemetry gauge,min=-3,max=12,sum=87,count=12 {0}" \
            .format(int(self._test_timestamp_millis))

        self.assertEqual(MetricExportResult.SUCCESS, result)
        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data=expected,
            headers=self._headers)

    @patch.object(requests.Session, 'post')
    def test_view(self, mock_post):
        mock_post.return_value = self._get_session_response()

        exporter = _DynatraceMetricsExporter()

        metric_reader = PeriodicExportingMetricReader(
            export_interval_millis=3600000,
            # 1h so that the test can finish before the collection event fires.
            exporter=exporter)

        meter_provider = MeterProvider(metric_readers=[metric_reader],
                                       views=[View(name="my.renamed.instr",
                                                   instrument_name=self._instrument_name)])

        meter = meter_provider.get_meter(name="my.meter", version="1.0.0")
        counter = meter.create_counter(self._instrument_name)
        counter.add(10, attributes={"l1": "v1", "l2": "v2"})

        metric_reader.collect()

        mock_post.assert_called_once_with(
            self._ingest_endpoint,
            data=AnyStringMatching(
                r"my\.renamed\.instr,(l2=v2,l1=v1|l1=v1,l2=v2),"
                r"dt\.metrics\.source=opentelemetry count,delta=10 [0-9]*"),
            headers=self._headers)

        counter.add(10, attributes={"l1": "v1", "l2": "v2"})

        # shut down cleanly to avoid failed exports later.
        meter_provider.shutdown()

    def test_configuration_default(self):
        with patch.object(PeriodicExportingMetricReader,
                          "__init__") as mock_reader:
            with patch.object(_DynatraceMetricsExporter,
                              "__init__") as mock_exporter:
                mock_reader.return_value = None
                mock_exporter.return_value = None
                self.assertIsInstance(configure_dynatrace_metrics_export(),
                                      PeriodicExportingMetricReader)
                mock_exporter.assert_called_once_with(
                    endpoint_url=None,
                    api_token=None,
                    prefix=None,
                    default_dimensions=None,
                    export_dynatrace_metadata=False,
                )
                mock_reader.assert_called_once_with(
                    export_interval_millis=None,
                    exporter=mock.ANY,
                )
                _, kwargs = mock_reader.call_args
                self.assertIsInstance(kwargs.get("exporter"),
                                      _DynatraceMetricsExporter)

    def test_configuration_custom(self):
        with patch.object(PeriodicExportingMetricReader,
                          "__init__") as mock_reader:
            with patch.object(_DynatraceMetricsExporter,
                              "__init__") as mock_exporter:
                mock_reader.return_value = None
                mock_exporter.return_value = None
                self.assertIsInstance(configure_dynatrace_metrics_export(
                    endpoint_url="endpoint.url",
                    export_dynatrace_metadata=True,
                    export_interval_millis=100,
                    api_token="dt.APItoken",
                    prefix="otel.python.test",
                    default_dimensions={"defaultKey": "defaultValue"}
                ),
                    PeriodicExportingMetricReader)
                mock_exporter.assert_called_once_with(
                    endpoint_url="endpoint.url",
                    api_token="dt.APItoken",
                    prefix="otel.python.test",
                    default_dimensions={"defaultKey": "defaultValue"},
                    export_dynatrace_metadata=True,
                )
                mock_reader.assert_called_once_with(
                    export_interval_millis=100,
                    exporter=mock.ANY,
                )
                _, kwargs = mock_reader.call_args
                self.assertIsInstance(kwargs.get("exporter"),
                                      _DynatraceMetricsExporter)

    def _metrics_data_from_metrics(self,
                                   metrics: Sequence[Metric]) -> MetricsData:
        return MetricsData(resource_metrics=[
            ResourceMetrics(
                resource=Resource({}),
                schema_url="http://schema.url/resource",
                scope_metrics=[ScopeMetrics(
                    scope=InstrumentationScope(
                        name="dynatrace.opentelemetry.metrics.export",
                        version="0.0.1"),
                    schema_url="http://schema.url/scope",
                    metrics=metrics
                )]
            )])

    def _metrics_data_from_data(self, data: Sequence[DataT]) -> MetricsData:
        return self._metrics_data_from_metrics(
            [self._metric_from_data(item) for item in data]
        )

    def _metric_from_data(self, data: DataT, instrument_name=None):
        return Metric(
            name=instrument_name
            if instrument_name is not None
            else self._instrument_name,
            description="",
            unit="1",
            data=data
        )

    def _create_sum(self, value: int, monotonic=True,
                    aggregation_temporality: AggregationTemporality = AggregationTemporality.DELTA) -> Sum:
        return Sum(
            is_monotonic=monotonic,
            aggregation_temporality=aggregation_temporality,
            data_points=[
                NumberDataPoint(
                    start_time_unix_nano=self._test_timestamp_nanos,
                    time_unix_nano=self._test_timestamp_nanos,
                    value=value,
                    attributes=self._attributes
                )
            ])

    def _create_gauge(self, value: int) -> Gauge:
        return Gauge(
            data_points=[
                NumberDataPoint(
                    start_time_unix_nano=self._test_timestamp_nanos,
                    time_unix_nano=self._test_timestamp_nanos,
                    value=value,
                    attributes=self._attributes
                )
            ])

    def _create_histogram(self,
                          bucket_counts: Sequence[int],
                          explicit_bounds: Sequence[int],
                          histogram_sum: Union[int, float] = 0,
                          histogram_min: Union[int, float] = 0,
                          histogram_max: Union[int, float] = 0,
                          aggregation_temporality: AggregationTemporality = AggregationTemporality.DELTA) -> Histogram:
        return Histogram(
            data_points=[
                HistogramDataPoint(
                    attributes=self._attributes,
                    bucket_counts=bucket_counts,
                    explicit_bounds=explicit_bounds,
                    count=sum(bucket_counts),
                    sum=histogram_sum,
                    min=histogram_min,
                    max=histogram_max,
                    time_unix_nano=self._test_timestamp_nanos,
                    start_time_unix_nano=self._test_timestamp_nanos,
                )
            ],
            aggregation_temporality=aggregation_temporality,
        )

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
