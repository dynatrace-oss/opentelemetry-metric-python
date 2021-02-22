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
import logging
import unittest
from unittest.mock import patch

from dynatrace.opentelemetry.metrics.export import OneAgentMetadataEnricher


class TestOneAgentMetadataEnricher(unittest.TestCase):
    __logger = logging.Logger(__name__)

    def test_parse_oneagent_metadata(self):
        enricher = OneAgentMetadataEnricher(self.__logger)
        parsed = enricher._parse_oneagent_metadata(["key1=value1", "key2=value2"])

        self.assertEqual("value1", parsed["key1"])
        self.assertEqual("value2", parsed["key2"])

    def test_parse_invalid_metadata(self):
        enricher = OneAgentMetadataEnricher(self.__logger)

        self.assertFalse(enricher._parse_oneagent_metadata(["=0x5c14d9a68d569861"]))
        self.assertFalse(enricher._parse_oneagent_metadata(["otherKey="]))
        self.assertFalse(enricher._parse_oneagent_metadata([""]))
        self.assertFalse(enricher._parse_oneagent_metadata(["="]))
        self.assertFalse(enricher._parse_oneagent_metadata(["==="]))
        self.assertFalse(enricher._parse_oneagent_metadata([]))


class TestExportToTags(unittest.TestCase):
    @patch('dynatrace.opentelemetry.metrics.export.oneagentmetadataenricher.OneAgentMetadataEnricher'
           '._parse_oneagent_metadata')
    def test_mock_get_metadata_file(self, mock_func):
        mock_func.return_value = {"k1": "v1", "k2": "v2"}

        enricher = OneAgentMetadataEnricher(logging.Logger(__name__))
        # put something in the map to make sure items are added and not overwritten.
        tags = {"tag1": "value1"}
        enricher.add_oneagent_metadata_to_tags(tags)

        self.assertEqual(tags, {"tag1": "value1", "k1": "v1", "k2": "v2"})

    @patch('dynatrace.opentelemetry.metrics.export.oneagentmetadataenricher.OneAgentMetadataEnricher'
           '._parse_oneagent_metadata')
    def test_tags_overwritten(self, mock_func):
        enricher = OneAgentMetadataEnricher(logging.Logger(__name__))
        tags = {"tag1": "value1"}
        mock_func.return_value = {"tag1": "newValue"}

        enricher.add_oneagent_metadata_to_tags(tags)

        self.assertEqual(tags, {"tag1": "newValue"})
