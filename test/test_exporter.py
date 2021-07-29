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

from dynatrace.opentelemetry.metrics.export import DynatraceMetricsExporter


class TestExporterCreation(unittest.TestCase):
    def test_all_optional(self):
        exporter = DynatraceMetricsExporter()
        self.assertEqual("http://localhost:14499/metrics/ingest",
                         exporter._endpoint_url)
        self.assertEqual(3, len(exporter._headers))
        self.assertNotIn("Authorization", exporter._headers)
        serializer = exporter._serializer
        self.assertEqual(None, serializer._prefix)
        self.assertDictEqual({}, serializer._default_dimensions)
        self.assertDictEqual({"dt.metrics.source": "opentelemetry"},
                             serializer._static_dimensions)

    def test_with_endpoint(self):
        endpoint = "https://abc1234.dynatrace.com/metrics/ingest"
        exporter = DynatraceMetricsExporter(endpoint_url=endpoint)

        self.assertEqual(endpoint, exporter._endpoint_url)
        self.assertEqual(3, len(exporter._headers))
        self.assertNotIn("Authorization", exporter._headers)
        serializer = exporter._serializer
        self.assertEqual(None, serializer._prefix)
        self.assertDictEqual({}, serializer._default_dimensions)
        self.assertDictEqual({"dt.metrics.source": "opentelemetry"},
                             serializer._static_dimensions)

    def test_has_useragent_header(self):
        endpoint = "https://abc1234.dynatrace.com/metrics/ingest"
        exporter = DynatraceMetricsExporter(endpoint_url=endpoint)

        self.assertEqual(endpoint, exporter._endpoint_url)
        self.assertEqual(3, len(exporter._headers))
        self.assertIn("User-Agent", exporter._headers)
        self.assertEqual(exporter._headers["User-Agent"],
                         "opentelemetry-metric-python")

    def test_with_endpoint_and_token(self):
        endpoint = "https://abc1234.dynatrace.com/metrics/ingest"
        token = "my.secret.token"
        exporter = DynatraceMetricsExporter(endpoint_url=endpoint,
                                            api_token=token)

        self.assertEqual(endpoint, exporter._endpoint_url)
        self.assertEqual(4, len(exporter._headers))
        self.assertEqual("Api-Token {}".format(token),
                         exporter._headers["Authorization"])
        serializer = exporter._serializer
        self.assertEqual(None, serializer._prefix)
        self.assertDictEqual({}, serializer._default_dimensions)
        self.assertDictEqual({"dt.metrics.source": "opentelemetry"},
                             serializer._static_dimensions)

    def test_with_only_token(self):
        token = "my.secret.token"
        exporter = DynatraceMetricsExporter(api_token=token)

        self.assertEqual("http://localhost:14499/metrics/ingest",
                         exporter._endpoint_url)
        self.assertEqual(3, len(exporter._headers))
        self.assertNotIn("Authorization", exporter._headers)
        serializer = exporter._serializer
        self.assertEqual(None, serializer._prefix)
        self.assertDictEqual({}, serializer._default_dimensions)
        self.assertDictEqual({"dt.metrics.source": "opentelemetry"},
                             serializer._static_dimensions)

    def test_with_prefix(self):
        prefix = "test_prefix"
        exporter = DynatraceMetricsExporter(prefix=prefix)

        serializer = exporter._serializer
        self.assertEqual(prefix, serializer._prefix)

    def test_with_tags(self):
        tags = {"tag1": "tv1", "tag2": "tv2"}
        exporter = DynatraceMetricsExporter(default_dimensions=tags)

        serializer = exporter._serializer

        expected = {"tag1": "tv1", "tag2": "tv2"}
        self.assertDictEqual(expected, serializer._default_dimensions)

    def test_with_none_tags(self):
        exporter = DynatraceMetricsExporter(default_dimensions=None)

        serializer = exporter._serializer
        self.assertDictEqual({}, serializer._default_dimensions)

    @patch('dynatrace.opentelemetry.metrics.export.dynatracemetadataenricher'
           '.DynatraceMetadataEnricher._get_metadata_file_content')
    def test_dynatrace_metadata_enrichment_valid_tags(self, mock_func):
        mock_func.return_value = [
            "dynatrace_metadatatag1=dynatrace_metadatavalue1",
            "dynatrace_metadatatag2=dynatrace_metadatavalue2"]
        expected = {"dynatrace_metadatatag1": "dynatrace_metadatavalue1",
                    "dynatrace_metadatatag2": "dynatrace_metadatavalue2",
                    "dt.metrics.source": "opentelemetry"}

        exporter = DynatraceMetricsExporter(export_dynatrace_metadata=True)
        serializer = exporter._serializer
        self.assertDictEqual(expected, serializer._static_dimensions)

    @patch('dynatrace.opentelemetry.metrics.export.dynatracemetadataenricher'
           '.DynatraceMetadataEnricher._get_metadata_file_content')
    def test_dynatrace_metadata_enrichment_empty_tags(self, mock_func):
        mock_func.return_value = []

        exporter = DynatraceMetricsExporter(export_dynatrace_metadata=True)
        serializer = exporter._serializer
        self.assertDictEqual({}, serializer._default_dimensions)

    @patch('dynatrace.opentelemetry.metrics.export.dynatracemetadataenricher'
           '.DynatraceMetadataEnricher._get_metadata_file_content')
    def test_dynatrace_metadata_enrichment_empty_add_to_tags(self, mock_func):
        mock_func.return_value = [
            "dynatrace_metadatatag1=dynatrace_metadatavalue1",
            "dynatrace_metadatatag2=dynatrace_metadatavalue2"]
        dimensions = {"tag1": "tv1", "tag2": "tv2"}

        expected_default = {"tag1": "tv1", "tag2": "tv2"}
        expected_static = {
            "dynatrace_metadatatag1": "dynatrace_metadatavalue1",
            "dynatrace_metadatatag2": "dynatrace_metadatavalue2",
            "dt.metrics.source": "opentelemetry"}

        exporter = DynatraceMetricsExporter(default_dimensions=dimensions,
                                            export_dynatrace_metadata=True)
        serializer = exporter._serializer
        self.assertDictEqual(expected_static,
                             serializer._static_dimensions)
        self.assertDictEqual(expected_default, serializer._default_dimensions)


if __name__ == '__main__':
    unittest.main()
