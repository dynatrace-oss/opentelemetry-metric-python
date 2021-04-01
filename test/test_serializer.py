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
from collections import OrderedDict
from typing import Union
from unittest import mock

from dynatrace.opentelemetry.metrics.export import serializer
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import aggregate, MetricRecord
from opentelemetry.sdk.resources import Resource


class TestDynatraceMetricsSerializer(unittest.TestCase):
    def setUp(self) -> None:
        self._meter_provider = MeterProvider()
        self._serializer = serializer.DynatraceMetricsSerializer(None, None)
        self._metric = DummyMetric("my.instr")
        self._labels = (("l1", "v1"), ("l2", "v2"))

        patcher = mock.patch.object(
            serializer, "get_meter_provider", return_value=self._meter_provider
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _create_record(self, aggregator: aggregate.Aggregator):
        return MetricRecord(
            self._metric, self._labels, aggregator, Resource({})
        )

    @staticmethod
    def _update_value(
        aggregator: aggregate.Aggregator,
        value: Union[int, float],
        time_stamp_ms: int = 555,
    ):
        aggregator.update(value)
        aggregator.take_checkpoint()
        aggregator.last_update_timestamp = time_stamp_ms * 1000000

    def test_sum_aggregator_absolute(self):
        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10)
        record = self._create_record(aggregator)
        serialized = self._serializer.serialize_records([record])

        self.assertEqual("my.instr,l1=v1,l2=v2 count,10 555\n", serialized)

    def test_sum_aggregator_delta(self):
        self._meter_provider.stateful = False
        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10)

        record = self._create_record(aggregator)
        result = self._serializer.serialize_records([record])

        self.assertEqual("my.instr,l1=v1,l2=v2 count,delta=10 555\n", result)

    def test_min_max_sum_count_aggregator(self):
        aggregator = aggregate.MinMaxSumCountAggregator()
        aggregator.update(100)
        aggregator.update(1)
        self._update_value(aggregator, 10, time_stamp_ms=999)

        record = self._create_record(aggregator)
        result = self._serializer.serialize_records([record])

        self.assertEqual(
            "my.instr,l1=v1,l2=v2 gauge,min=1,max=100,sum=111,count=3 999\n",
            result
        )

    def test_last_value_aggregator(self):
        aggregator = aggregate.LastValueAggregator()
        self._update_value(aggregator, 20, time_stamp_ms=777)

        record = self._create_record(aggregator)
        result = self._serializer.serialize_records([record])

        self.assertEqual("my.instr,l1=v1,l2=v2 count,20 777\n", result)

    def test_multiple_records(self):
        records = []
        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10)
        records.append(self._create_record(aggregator))

        aggregator = aggregate.MinMaxSumCountAggregator()
        aggregator.update(100)
        aggregator.update(1)
        self._update_value(aggregator, 10, time_stamp_ms=999)
        records.append(self._create_record(aggregator))

        aggregator = aggregate.LastValueAggregator()
        self._update_value(aggregator, 20, time_stamp_ms=777)
        records.append(self._create_record(aggregator))

        serialized = self._serializer.serialize_records(records)

        self.assertEqual(
            "my.instr,l1=v1,l2=v2 count,10 555\n"
            "my.instr,l1=v1,l2=v2 gauge,min=1,max=100,sum=111,count=3 999\n"
            "my.instr,l1=v1,l2=v2 count,20 777\n",
            serialized,
        )

    def test_without_labels(self):
        self._labels = ()
        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10)
        record = self._create_record(aggregator)
        serialized = self._serializer.serialize_records([record])

        self.assertEqual("my.instr count,10 555\n", serialized)

    def test_prefix(self):
        prefix = "prefix"
        self._serializer = serializer.DynatraceMetricsSerializer(prefix, None)
        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10, time_stamp_ms=111)

        record = self._create_record(aggregator)
        result = self._serializer.serialize_records([record])

        self.assertEqual("prefix.my.instr,l1=v1,l2=v2 count,10 111\n", result)

    def test_tags(self):
        dimensions = {"t1": "tv1", "t2": "tv2"}
        self._serializer = serializer.DynatraceMetricsSerializer(None,
                                                                 dimensions)

        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10, time_stamp_ms=111)

        record = self._create_record(aggregator)
        result = self._serializer.serialize_records([record])

        self.assertEqual("my.instr,t1=tv1,t2=tv2,l1=v1,l2=v2 count,10 111\n",
                         result)

    def test_invalid_name(self):
        metric = DummyMetric(".")
        aggregator = aggregate.SumAggregator()
        record = MetricRecord(
            metric, self._labels, aggregator, Resource({})
        )

        self._serializer = serializer.DynatraceMetricsSerializer()

        self._update_value(aggregator, 10, time_stamp_ms=111)

        result = self._serializer.serialize_records([record])

        self.assertEqual(
            "", result
        )

    def test_write_valid_dimensions(self):
        dimensions = {"dim1": "value1", "dim2": "value2"}
        target = []
        serializer.DynatraceMetricsSerializer._write_dimensions(target,
                                                                dimensions)

        # 8 because the commas and the equal signs are separate strings
        self.assertEqual(8, len(target))
        self.assertEqual(",dim1=value1,dim2=value2", "".join(target))

    def test_make_unique_dimensions_empty(self):
        default_dims = {}
        user_dims = {}
        oneagent_dims = {}

        expected = {}
        got = serializer.DynatraceMetricsSerializer._make_unique_dimensions(
            default_dims, user_dims, oneagent_dims)

        self.assertDictEqual(expected, got)

    def test_make_unique_dimensions_valid(self):
        default_dims = {"dim1": "dv1", "dim2": "dv2"}
        user_dims = [("tag1", "tv1"), ("tag2", "tv2")]
        oneagent_dims = {"one1": "val1", "one2": "val2"}

        expected = {"tag1": "tv1", "tag2": "tv2", "dim1": "dv1", "dim2": "dv2",
                    "one1": "val1", "one2": "val2"}
        got = serializer.DynatraceMetricsSerializer._make_unique_dimensions(
            default_dims, user_dims, oneagent_dims)

        self.assertDictEqual(expected, got)

    def test_make_unique_dimensions_overwrite(self):
        defaultDims = {"dim1": "defv1", "dim2": "defv2", "dim3": "defv3"}
        dimensions = [("dim1", "dimv2"), ("dim2", "dimv2")]
        oneAgentDims = {"dim1": "onev1"}

        expected = {"dim1": "onev1", "dim2": "dimv2", "dim3": "defv3"}
        got = serializer.DynatraceMetricsSerializer._make_unique_dimensions(
            defaultDims, dimensions, oneAgentDims)
        self.assertDictEqual(expected, got)

    def test_make_unique_dimensions_overwrite_after_normalization(self):
        # we assume the default dimensions and OneAgent dims to be
        # well-formed here as that is done in the  constructor of the
        # DynatraceMetricsExporter, so no malformed tags  should ever be
        # passed to this function.
        default_dims = {"dim1": "defv1", "dim2": "defv2"}
        dims = [("~~!@$dim1", "dimv1"), ("@#$$%dim2", "dimv2")]
        oneagent_dims = {}

        expected = {"dim1": "dimv1", "dim2": "dimv2"}
        got = serializer.DynatraceMetricsSerializer._make_unique_dimensions(
            default_dims, dims, oneagent_dims)

        self.assertDictEqual(expected, got)

    def test_normalize_dimensions(self):
        tags = {"tag1": "tv1", "tag2": "tv2"}
        expected = {"tag1": "tv1", "tag2": "tv2"}

        got = serializer.DynatraceMetricsSerializer._normalize_dimensions(tags)
        self.assertDictEqual(expected, got)

    def test_normalize_dimensions_invalid(self):
        tags = {"@!#~tag1": "  \"val\"", "%%tag2@@": "", "": "empty"}
        expected = {"tag1": "\\ \\ \"val\"", "tag2": ""}

        got = serializer.DynatraceMetricsSerializer._normalize_dimensions(tags)
        self.assertDictEqual(expected, got)

    def test_normalize_dimensions_pass_none_or_empty(self):
        expected = {}
        dimensions1 = None

        got1 = serializer.DynatraceMetricsSerializer._normalize_dimensions(
            dimensions1)
        self.assertDictEqual(expected, got1)

        dimensions2 = {}
        got2 = serializer.DynatraceMetricsSerializer._normalize_dimensions(
            dimensions2)
        self.assertDictEqual(expected, got2)


class DummyMetric:
    def __init__(self, name: str):
        self.name = name
