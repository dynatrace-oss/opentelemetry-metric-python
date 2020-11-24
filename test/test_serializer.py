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
        tags = OrderedDict()
        tags["t1"] = "tv1"
        tags["t2"] = "tv2"
        self._serializer = serializer.DynatraceMetricsSerializer(None, tags)

        aggregator = aggregate.SumAggregator()
        self._update_value(aggregator, 10, time_stamp_ms=111)

        record = self._create_record(aggregator)
        result = self._serializer.serialize_records([record])

        self.assertEqual(
            "my.instr,l1=v1,l2=v2,t1=tv1,t2=tv2 count,10 111\n", result
        )

    def test_invalid_name(self):
        metric = DummyMetric(".")
        aggregator = aggregate.SumAggregator()
        record = MetricRecord(
            metric, self._labels, aggregator, Resource({})
        )

        self._serializer = serializer.DynatraceMetricsSerializer(None, None)

        self._update_value(aggregator, 10, time_stamp_ms=111)

        result = self._serializer.serialize_records([record])

        self.assertEqual(
            "", result
        )

    def test_normalize_metric_key(self):
        cases = [
            ["basecase", "basecase"],
            ["prefix.case", "prefix.case"],
            ["", ""],
            ["0", ""],
            ["0.section", ""],
            ["just.a.normal.key", "just.a.normal.key"],
			["Case", "Case"],
			["~0something", "something"],
			["some~thing", "some_thing"],
			["some~ä#thing", "some_thing"],
			["a..b", "a.b"],
			["a.....b", "a.b"],
			["asd", "asd"],
			[".", ""],
			[".a", ""],
			["a.", "a"],
			[".a.", ""],
			["_a", "a"],
			["a_", "a_"],
			["_a_", "a_"],
			[".a_", ""],
			["_a.", "a"],
			["._._a_._._", ""],
			["test..empty.test", "test.empty.test"],
			["a,,,b  c=d\\e\\ =,f", "a_b_c_d_e_f"],
			["a!b\"c#d$e%f&g'h(i)j*k+l,m-n.o/p:q;r<s=t>u?v@w[x]y\\z^0 1_2;3{4|5}6~7", "a_b_c_d_e_f_g_h_i_j_k_l_m-n.o_p_q_r_s_t_u_v_w_x_y_z_0_1_2_3_4_5_6_7"],
        ]
        for case in cases:
            self.assertEqual(
                serializer.DynatraceMetricsSerializer._normalize_metric_key(case[0]), case[1])


class DummyMetric:
    def __init__(self, name: str):
        self.name = name
