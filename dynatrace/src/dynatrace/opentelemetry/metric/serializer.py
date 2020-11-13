from typing import Callable, Iterable, List, Mapping, Optional, Sequence, Tuple

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

        self._write_metric_key(string_buffer, record)
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
        string_buffer: List[str], aggregator: aggregate.MinMaxSumCountAggregator
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

    def _write_metric_key(
        self,
        string_buffer: List[str],
        record: MetricRecord,
    ):
        metric_key = record.instrument.name
        if self._prefix:
            metric_key = self._prefix + "." + metric_key
        string_buffer.append(metric_key)

    @staticmethod
    def _write_dimensions(
        string_buffer: List[str], dimensions: Iterable[Tuple[str, str]]
    ):
        for key, value in dimensions:
            string_buffer.append(",")
            string_buffer.append(key)
            string_buffer.append("=")
            string_buffer.append(value)

    @staticmethod
    def _write_timestamp(sb: List[str], aggregator: aggregate.Aggregator):
        sb.append(" ")
        # nanos to millis
        sb.append(str(aggregator.last_update_timestamp // 1000000))
