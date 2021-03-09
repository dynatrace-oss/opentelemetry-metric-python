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
import unicodedata

from opentelemetry.metrics import get_meter_provider
from opentelemetry.sdk.metrics.export import aggregate, MetricRecord


def _determine_is_delta_export():
    meter_provider = get_meter_provider()
    return hasattr(meter_provider, "stateful") and not meter_provider.stateful


class DynatraceMetricsSerializer:
    # Metric keys (mk)
    # characters not valid to start the first identifier key section
    __re_mk_first_identifier_section_start = (re.compile(r"^[^a-zA-Z_]+"))

    # characters not valid to start subsequent identifier key sections
    __re_mk_identifier_section_start = re.compile(r"^[^a-zA-Z0-9_]+")
    __re_mk_identifier_section_end = re.compile(r"[^a-zA-Z0-9_\-]+$")

    # for the rest of the metric key characters, alphanumeric characters as
    # well as hyphens and underscores are allowed. consecutive invalid
    # characters will be condensed into one underscore.
    __re_mk_invalid_characters = re.compile(r"[^a-zA-Z0-9_\-]+")

    __mk_max_length = 250

    # Dimension keys (dk)
    # dimension keys have to start with a lowercase letter or an underscore.
    __re_dk_start = re.compile(r"^[^a-z_]+")
    __re_dk_end = re.compile(r"[^a-z0-9_\-:]+$")

    # other valid characters in dimension keys are lowercase letters, numbers,
    # colons, underscores and hyphens.
    __re_dk_invalid_chars = re.compile(r"[^a-z0-9_\-:]+")

    __dk_max_length = 100

    # Dimension values (dv)
    # all control characters (cc) are replaced with the null character and then
    # removed as appropriate using the following regular expressions.
    __re_dv_cc = re.compile(r"\u0000+")
    __re_dv_cc_leading = re.compile(r"^" + __re_dv_cc.pattern)
    __re_dv_cc_trailing = re.compile(__re_dv_cc.pattern + r"$")

    # characters to be escaped in the dimension value
    __re_dv_escape_chars = re.compile(r"([= ,\\])")

    __dv_max_length = 250

    def __init__(
        self,
        prefix: Optional[str],
        tags: Optional[Mapping],
    ):
        self._prefix = prefix
        self._is_delta_export = None

        self._tags = self._normalize_tags(tags)

    @classmethod
    def _normalize_tags(cls, tags):
        tag_dict = {}
        if tags:
            # normalize the tags once, so it doesnt have to be repeated for
            # every serialisation
            for k, v in tags.items():
                key = cls._normalize_dimension_key(k)
                if key:
                    tag_dict[key] = cls._normalize_dimension_value(v)

        return tag_dict

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

        unique_dimensions = self._make_unique_dimensions(record.labels,
                                                         self._tags.items())
        DynatraceMetricsSerializer._write_dimensions(string_buffer,
                                                     unique_dimensions)

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

    # characters not valid to start the first identifier key section
    __re_metric_key_first_identifier_section_start = re.compile(r"^[^a-zA-Z_]+")

    # characters not valid to start subsequent identifier key sections
    __re_metric_key_identifier_section_start = re.compile(r"^[^a-zA-Z0-9_]+")

    # for the rest of the metric key characters, alphanumeric characters as
    # well as hyphens and underscores are allowed. consecutive invalid
    # characters will be condensed into one underscore.
    __re_metric_key_invalid_characters = re.compile(r"[^a-zA-Z0-9_\-]+")

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

    @classmethod
    def __normalize_metric_key_first_section(cls, section: str) -> str:
        return DynatraceMetricsSerializer.__normalize_metric_key_section(
            # delete invalid characters for first section start
            cls.__re_mk_first_identifier_section_start.sub("", section)
        )

    @classmethod
    def __normalize_metric_key_section(cls, section: str) -> str:
        # delete invalid characters at the start of the section key
        section = cls.__re_mk_identifier_section_start.sub("", section)
        # delete invalid characters at the end of the section key
        section = cls.__re_mk_identifier_section_end.sub("", section)
        # replace ranges of invalid characters in the key with one underscore.
        section = cls.__re_mk_invalid_characters.sub("_", section)
        return section

    @classmethod
    def _normalize_dimension_key(cls, key: str):
        # truncate dimension key to max length.
        key = key[:cls.__dk_max_length]

        # separate sections
        sections = key.split(".")
        # normalize them and drop empty sections using filter
        normalized = list(filter(None, map(
            DynatraceMetricsSerializer.__normalize_dimension_key_section,
            sections
        )))

        return ".".join(normalized)

    @classmethod
    def __normalize_dimension_key_section(cls, section: str):
        # convert to lowercase
        section = section.lower()
        # delete leading invalid characters
        section = cls.__re_dk_start.sub("", section)
        # delete trailing invalid characters
        section = cls.__re_dk_end.sub("", section)
        # replace consecutive invalid characters with one underscore:
        section = cls.__re_dk_invalid_chars.sub("_", section)

        return section

    __dimension_key_max_length = 100

    @classmethod
    def _normalize_dimension_key(cls, key: str):
        # truncate dimension key to max length.
        key = key[:cls.__dimension_key_max_length]

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
    __re_dimension_key_end = re.compile(r"[^a-z0-9_\-:]+$")

    # other valid characters in dimension keys are lowercase letters, numbers,
    # colons, underscores and hyphens.
    __re_dimension_key_invalid_chars = re.compile(r"[^a-z0-9_\-:]+")

    @classmethod
    def __normalize_dimension_key_section(cls, section: str):
        # convert to lowercase
        section = section.lower()
        # delete leading invalid characters
        section = cls.__re_dimension_key_start.sub("", section)
        # delete trailing invalid characters
        section = cls.__re_dimension_key_end.sub("", section)
        # replace consecutive invalid characters with one underscore:
        section = cls.__re_dimension_key_invalid_chars.sub("_", section)

        return section

    @staticmethod
    def _write_dimensions(
        string_buffer: List[str], dimensions: Mapping[str, str]
    ):
        # when the calling function uses make unique dimensions first, keys and
        # values are already normalized to ensure correct duplicate elimination
        for k, v in dimensions.items():
            string_buffer.append(",")
            string_buffer.append(k)
            string_buffer.append("=")
            string_buffer.append(v)

    @staticmethod
    def _write_timestamp(sb: List[str], aggregator: aggregate.Aggregator):
        sb.append(" ")
        # nanos to millis
        sb.append(str(aggregator.last_update_timestamp // 1000000))

    @classmethod
    def _remove_control_characters(cls, s: str) -> str:
        # replace all control chars with null char
        s = "".join(
            c if unicodedata.category(c)[0] != "C" else "\u0000" for c in s)

        # then delete leading and trailing ranges of null chars
        s = cls.__re_dv_cc_leading.sub("", s)
        s = cls.__re_dv_cc_trailing.sub("", s)
        # and replace enclosed ranges of null chars with one underscore.
        s = cls.__re_dv_cc.sub("_", s)
        return s

    __metric_value_max_length = 250

    @classmethod
    def _normalize_dimension_value(cls, value: str):
        value = value[:cls.__dv_max_length]
        value = DynatraceMetricsSerializer._remove_control_characters(value)
        value = cls.__re_dv_escape_chars.sub(r"\\\g<1>", value)
        return value

    @classmethod
    def _make_unique_dimensions(cls, labels: Iterable[Tuple[str, str]],
                                tags: Iterable[Tuple[str, str]]):
        dims_map = {}
        if labels:
            for k, v in labels:
                key = cls._normalize_dimension_key(k)
                if key:
                    dims_map[key] = cls._normalize_dimension_value(v)

        # overwrite tags that the user set with the static tags and OneAgent
        # metadata. Tags are normalized in __init__ so they don't have to be
        # re-normalized here.
        if tags:
            for k, v in tags:
                dims_map[k] = v

        return dims_map
