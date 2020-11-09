from typing import Callable, Iterable, List, Mapping, Optional, Sequence, Tuple

from opentelemetry.sdk.metrics.export import aggregate, MetricRecord


def serialize_records(
    records: Sequence[MetricRecord],
    prefix: Optional[str] = None,
    tags: Optional[Mapping[str, str]] = None
):
    string_buffer = []  # type: List[str]
    for record in records:
        _write_record(string_buffer, record, prefix, tags)

    return "".join(string_buffer)


def _write_record(
    string_buffer: List[str],
    record: MetricRecord,
    prefix: Optional[str],
    tags: Optional[Mapping[str, str]],
):
    aggregator = record.aggregator
    serialize_func = _get_serialize_func(aggregator)

    if serialize_func is None:
        return

    _write_metric_key(string_buffer, record, prefix)
    _write_dimensions(string_buffer, record.labels)
    if tags:
        _write_dimensions(string_buffer, tags.items())

    serialize_func(string_buffer, aggregator)

    _write_timestamp(string_buffer, aggregator)
    string_buffer.append("\n")


def _get_serialize_func(
    aggregator: aggregate.Aggregator
) -> Optional[Callable]:
    if isinstance(aggregator, aggregate.SumAggregator):
        return _write_count_value
    if isinstance(aggregator, aggregate.MinMaxSumCountAggregator):
        return _write_gauge_value
    if isinstance(aggregator, aggregate.ValueObserverAggregator):
        return _write_gauge_value
    if isinstance(aggregator, aggregate.LastValueAggregator):
        return None  # Not supported
    if isinstance(aggregator, aggregate.HistogramAggregator):
        return None  # Not supported
    return None


def _write_count_value(
    string_buffer: List[str], aggregator: aggregate.SumAggregator
):
    string_buffer.append(" count,delta=")
    string_buffer.append(str(aggregator.checkpoint))


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
    string_buffer: List[str], record: MetricRecord, prefix: Optional[str],
):
    metric_key = record.instrument.name
    if prefix:
        metric_key = prefix + "." + metric_key
    string_buffer.append(metric_key)


def _write_dimensions(
    string_buffer: List[str], dimensions: Iterable[Tuple[str, str]]
):
    for key, value in dimensions:
        string_buffer.append(",")
        string_buffer.append(key)
        string_buffer.append("=")
        string_buffer.append(value)


def _write_timestamp(sb: List[str], aggregator: aggregate.Aggregator):
    sb.append(" ")
    # nanos to millis
    sb.append(str(aggregator.last_update_timestamp // 1000000))
