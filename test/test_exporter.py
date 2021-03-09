import unittest
from unittest.mock import patch

from dynatrace.opentelemetry.metrics.export import DynatraceMetricsExporter


class TestExporterCreation(unittest.TestCase):
    def test_all_optional(self):
        exporter = DynatraceMetricsExporter()
        self.assertEqual("http://localhost:14499/metrics/ingest",
                         exporter._endpoint_url)
        self.assertEqual(2, len(exporter._headers))
        self.assertNotIn("Authorization", exporter._headers)
        serializer = exporter._serializer
        self.assertEqual(None, serializer._prefix)
        self.assertDictEqual({}, serializer._tags)

    def test_with_endpoint(self):
        endpoint = "https://abc1234.dynatrace.com/metrics/ingest"
        exporter = DynatraceMetricsExporter(endpoint_url=endpoint)

        self.assertEqual(endpoint, exporter._endpoint_url)
        self.assertEqual(2, len(exporter._headers))
        self.assertNotIn("Authorization", exporter._headers)
        serializer = exporter._serializer
        self.assertEqual(None, serializer._prefix)
        self.assertDictEqual({}, serializer._tags)

    def test_with_endpoint_and_token(self):
        endpoint = "https://abc1234.dynatrace.com/metrics/ingest"
        token = "my.secret.token"
        exporter = DynatraceMetricsExporter(endpoint_url=endpoint,
                                            api_token=token)

        self.assertEqual(endpoint, exporter._endpoint_url)
        self.assertEqual(3, len(exporter._headers))
        self.assertEqual("Api-Token {}".format(token),
                         exporter._headers["Authorization"])
        serializer = exporter._serializer
        self.assertEqual(None, serializer._prefix)
        self.assertDictEqual({}, serializer._tags)

    def test_with_only_token(self):
        token = "my.secret.token"
        exporter = DynatraceMetricsExporter(api_token=token)

        self.assertEqual("http://localhost:14499/metrics/ingest",
                         exporter._endpoint_url)
        self.assertEqual(2, len(exporter._headers))
        self.assertNotIn("Authorization", exporter._headers)
        serializer = exporter._serializer
        self.assertEqual(None, serializer._prefix)
        self.assertDictEqual({}, serializer._tags)

    def test_with_prefix(self):
        prefix = "test_prefix"
        exporter = DynatraceMetricsExporter(prefix=prefix)

        serializer = exporter._serializer
        self.assertEqual(prefix, serializer._prefix)

    def test_with_tags(self):
        tags = {"tag1": "tv1", "tag2": "tv2"}
        exporter = DynatraceMetricsExporter(tags=tags)

        serializer = exporter._serializer
        self.assertEqual(tags, serializer._tags)

    def test_with_none_tags(self):
        exporter = DynatraceMetricsExporter(tags=None)

        serializer = exporter._serializer
        self.assertEqual({}, serializer._tags)

    @patch('dynatrace.opentelemetry.metrics.export.oneagentmetadataenricher'
           '.OneAgentMetadataEnricher._get_metadata_file_content')
    def test_oneagent_enrichment_valid_tags(self, mock_func):
        mock_func.return_value = ["oneagenttag1=oneagentvalue1",
                                  "oneagenttag2=oneagentvalue2"]
        expected = {"oneagenttag1": "oneagentvalue1",
                    "oneagenttag2": "oneagentvalue2"}

        exporter = DynatraceMetricsExporter(export_oneagent_metadata=True)
        serializer = exporter._serializer
        self.assertEqual(expected, serializer._tags)

    @patch('dynatrace.opentelemetry.metrics.export.oneagentmetadataenricher'
           '.OneAgentMetadataEnricher._get_metadata_file_content')
    def test_oneagent_enrichment_empty_tags(self, mock_func):
        mock_func.return_value = []

        exporter = DynatraceMetricsExporter(export_oneagent_metadata=True)
        serializer = exporter._serializer
        self.assertEqual({}, serializer._tags)

    @patch('dynatrace.opentelemetry.metrics.export.oneagentmetadataenricher'
           '.OneAgentMetadataEnricher._get_metadata_file_content')
    def test_oneagent_enrichment_empty_add_to_tags(self, mock_func):
        mock_func.return_value = ["oneagenttag1=oneagentvalue1",
                                  "oneagenttag2=oneagentvalue2"]
        tags = {"tag1": "tv1", "tag2": "tv2"}

        expected = {"tag1": "tv1", "tag2": "tv2",
                    "oneagenttag1": "oneagentvalue1",
                    "oneagenttag2": "oneagentvalue2"}

        exporter = DynatraceMetricsExporter(tags=tags,
                                            export_oneagent_metadata=True)
        serializer = exporter._serializer
        self.assertEqual(expected, serializer._tags)

    @patch('dynatrace.opentelemetry.metrics.export.oneagentmetadataenricher'
           '.OneAgentMetadataEnricher._get_metadata_file_content')
    def test_overwrite_tags(self, mock_func):
        mock_func.return_value = ["oneagenttag1=oneagentvalue1",
                                  "oneagenttag2=oneagentvalue2"]
        tags = {"oneagenttag1": "notactuallyoneagent", "tag1": "tv1"}

        expected = {"tag1": "tv1",
                    "oneagenttag1": "oneagentvalue1",
                    "oneagenttag2": "oneagentvalue2"}

        exporter = DynatraceMetricsExporter(tags=tags,
                                            export_oneagent_metadata=True)
        serializer = exporter._serializer
        self.assertEqual(expected, serializer._tags)


if __name__ == '__main__':
    unittest.main()
