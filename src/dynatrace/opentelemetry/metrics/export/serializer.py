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

from typing import Callable, Iterable, List, Mapping, Optional, Sequence, Tuple

import re

from opentelemetry.metrics import get_meter_provider
from opentelemetry.sdk.metrics.export import aggregate, MetricRecord


def _determine_is_delta_export():
    meter_provider = get_meter_provider()
    return hasattr(meter_provider, "stateful") and not meter_provider.stateful


class DynatraceMetricsSerializer:

    def __init__(
        self,
        prefix: Optional[str],
        tags: Optional[Mapping],
    ):
        self._prefix = prefix
        self._tags = tags or {}
        self._is_delta_export = None

    def serialize_records(
        self, records: Sequence[MetricRecord]
    ) -> str:
        if self._is_delta_export is None:
            self._is_delta_export = _determine_is_delta_export()

        string_buffer = []  # type: List[str]
        for record in records:
            self._write_record(string_buffer, record)

        return "".join(string_buffer)

    def _write_record(
        self,
        string_buffer: List[str],
        record: MetricRecord,
    ):
        aggregator = record.aggregator
        serialize_func = self._get_serialize_func(aggregator)

        if serialize_func is None:
            return

        metric_key = self._get_metric_key(record)
        if metric_key == "":
            return
        string_buffer.append(metric_key)
        self._write_dimensions(string_buffer, record.labels)
        if self._tags:
            self._write_dimensions(string_buffer, self._tags.items())

        serialize_func(string_buffer, aggregator)

        self._write_timestamp(string_buffer, aggregator)
        string_buffer.append("\n")

    def _get_serialize_func(
        self, aggregator: aggregate.Aggregator
    ) -> Optional[Callable]:
        if isinstance(aggregator, aggregate.SumAggregator):
            if self._is_delta_export:
                return self._write_count_value_delta
            return self._write_count_value_absolute
        if isinstance(aggregator, aggregate.MinMaxSumCountAggregator):
            return self._write_gauge_value
        if isinstance(aggregator, aggregate.ValueObserverAggregator):
            return self._write_gauge_value
        if isinstance(aggregator, aggregate.LastValueAggregator):
            return self._write_count_value_absolute
        if isinstance(aggregator, aggregate.HistogramAggregator):
            return None  # Not supported
        return None

    @staticmethod
    def _write_count_value_absolute(
        string_buffer: List[str],
        aggregator: aggregate.SumAggregator
    ):
        string_buffer.append(" count,")
        string_buffer.append(str(aggregator.checkpoint))

    @staticmethod
    def _write_count_value_delta(
        string_buffer: List[str],
        aggregator: aggregate.SumAggregator
    ):
        string_buffer.append(" count,delta=")
        string_buffer.append(str(aggregator.checkpoint))

    @staticmethod
    def _write_gauge_value(
        string_buffer: List[str],
        aggregator: aggregate.MinMaxSumCountAggregator,
    ):
        checkpoint = aggregator.checkpoint
        string_buffer.append(" gauge,min=")
        string_buffer.append(str(checkpoint.min))
        string_buffer.append(",max=")
        string_buffer.append(str(checkpoint.max))
        string_buffer.append(",sum=")
        string_buffer.append(str(checkpoint.sum))
        string_buffer.append(",count=")
        string_buffer.append(str(checkpoint.count))

    def _get_metric_key(
        self,
        record: MetricRecord,
    ) -> str:
        metric_key = record.instrument.name
        if self._prefix:
            metric_key = self._prefix + "." + metric_key
        return DynatraceMetricsSerializer._normalize_metric_key(metric_key)

    @staticmethod
    def _normalize_metric_key(key: str) -> str:
        first, *rest = key.split(".")

        first = (DynatraceMetricsSerializer.
                 __normalize_metric_key_first_section(first))

        if first == "":
            return ""

        rest = list(filter(None, map(
            DynatraceMetricsSerializer.__normalize_metric_key_section,
            rest,
        )))

        return ".".join([x for x in [first] + rest if x != ""])

    # characters not valid to start the first identifier key section
    __re_metric_key_first_identifier_section_start = (
        re.compile(r"^[^a-zA-Z_]+"))

    # characters not valid to start subsequent identifier key sections
    __re_metric_key_identifier_section_start = re.compile(r"^[^a-zA-Z0-9_]+")

    # for the rest of the metric key characters, alphanumeric characters as
    # well as hyphens and underscores are allowed. consecutive invalid
    # characters will be condensed into one underscore.
    __re_metric_key_invalid_characters = re.compile(r"[^a-zA-Z0-9_\-]+")

    @classmethod
    def __normalize_metric_key_first_section(cls, section: str) -> str:
        return DynatraceMetricsSerializer.__normalize_metric_key_section(
            # delete invalid characters for first section start
            cls.__re_metric_key_first_identifier_section_start.sub("", section)
        )

    @classmethod
    def __normalize_metric_key_section(cls, section: str) -> str:
        # delete invalid characters at the start of the section key
        section = cls.__re_metric_key_identifier_section_start.sub("", section)
        section = cls.__re_metric_key_invalid_characters.sub("_", section)
        return section

    @staticmethod
    def _normalize_dimension_key(key: str):
        # separate sections
        sections = key.split(".")
        # normalize them and drop empty sections using filter
        normalized = list(filter(None, map(
            DynatraceMetricsSerializer.__normalize_dimension_key_section,
            sections
        )))

        return ".".join(normalized)

    # dimension keys have to start with a lowercase letter or an underscore.
    __re_dimension_key_start = re.compile(r"^[^a-z_]+")

    # other valid characters in dimension keys are lowercase letters, numbers,
    # colons, underscores and hyphens.
    __re_dimension_key_invalid_chars = re.compile(r"[^a-z0-9_\-:]+")

    @classmethod
    def __normalize_dimension_key_section(cls, section: str):
        # convert to lowercase
        section = section.lower()
        # delete leading invalid characters
        section = cls.__re_dimension_key_start.sub("", section)
        # replace consecutive invalid characters with one underscore:
        section = cls.__re_dimension_key_invalid_chars.sub("_", section)

        return section

    @staticmethod
    def _write_dimensions(
        string_buffer: List[str], dimensions: Iterable[Tuple[str, str]]
    ):
        for key, value in dimensions:
            dim_key = DynatraceMetricsSerializer._normalize_dimension_key(key)
            if dim_key:
                dim_value = (
                    DynatraceMetricsSerializer.
                    _normalize_dimension_value(value))

                string_buffer.append(",")
                string_buffer.append(dim_key)
                string_buffer.append("=")
                string_buffer.append(dim_value)

            # else: the dimension is empty, the dimension is dropped.

    @staticmethod
    def _write_timestamp(sb: List[str], aggregator: aggregate.Aggregator):
        sb.append(" ")
        # nanos to millis
        sb.append(str(aggregator.last_update_timestamp // 1000000))

    __re_control_characters = re.compile(r"[\n\t\r]")
    __re_characters_to_escape = re.compile(r"([= ,\\])")

    @classmethod
    def _normalize_dimension_value(cls, value: str):
        value = cls.__re_control_characters.sub("", value)
        value = cls.__re_characters_to_escape.sub(r"\\\g<1>", value)
        return value
