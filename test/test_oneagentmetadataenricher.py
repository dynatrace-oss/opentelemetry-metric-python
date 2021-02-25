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
import os
import unittest
import tempfile
from unittest.mock import patch, mock_open

from dynatrace.opentelemetry.metrics.export import OneAgentMetadataEnricher


class TestOneAgentMetadataEnricher(unittest.TestCase):
    __logger = logging.Logger(__name__)

    def test_parse_oneagent_metadata(self):
        enricher = OneAgentMetadataEnricher()
        parsed = enricher._parse_oneagent_metadata(["key1=value1",
                                                    "key2=value2"])

        self.assertEqual("value1", parsed["key1"])
        self.assertEqual("value2", parsed["key2"])
        self.assertEqual(2, len(parsed))

    def test_parse_invalid_metadata(self):
        enricher = OneAgentMetadataEnricher()

        self.assertFalse(enricher._parse_oneagent_metadata(
            ["=0x5c14d9a68d569861"]))
        self.assertFalse(enricher._parse_oneagent_metadata(["otherKey="]))
        self.assertFalse(enricher._parse_oneagent_metadata([""]))
        self.assertFalse(enricher._parse_oneagent_metadata(["="]))
        self.assertFalse(enricher._parse_oneagent_metadata(["==="]))
        self.assertFalse(enricher._parse_oneagent_metadata([]))

    def test_valid_and_invalid(self):
        enricher = OneAgentMetadataEnricher()

        parsed = enricher._parse_oneagent_metadata([
            "validKey1=validValue1",
            "=invalidKey",
            "invalidValue=",
            "",
            "validKey2=validValue2"
        ])

        self.assertEqual(2, len(parsed))
        self.assertEqual("validValue1", parsed["validKey1"])
        self.assertEqual("validValue2", parsed["validKey2"])


class TestParseMetadata(unittest.TestCase):
    @patch('dynatrace.opentelemetry.metrics.export.oneagentmetadataenricher'
           '.OneAgentMetadataEnricher._parse_oneagent_metadata')
    def test_mock_get_metadata_file(self, mock_func):
        mock_func.return_value = {"k1": "v1", "k2": "v2"}

        enricher = OneAgentMetadataEnricher()
        # put something in the map to make sure items are added and not
        # overwritten.
        tags = {"tag1": "value1"}
        enricher.add_oneagent_metadata_to_tags(tags)

        self.assertEqual(tags, {"tag1": "value1", "k1": "v1", "k2": "v2"})

    @patch('dynatrace.opentelemetry.metrics.export.oneagentmetadataenricher'
           '.OneAgentMetadataEnricher._parse_oneagent_metadata')
    def test_tags_overwritten(self, mock_func):
        enricher = OneAgentMetadataEnricher()
        tags = {"tag1": "value1"}
        mock_func.return_value = {"tag1": "newValue"}

        enricher.add_oneagent_metadata_to_tags(tags)

        self.assertEqual(tags, {"tag1": "newValue"})

    def test_parse_valid_data(self):
        in_list = ["k1=v1\n", "k2=v2"]
        enricher = OneAgentMetadataEnricher()
        res = enricher._parse_oneagent_metadata(in_list)

        self.assertEqual(2, len(res))
        self.assertEqual("v1", res["k1"])
        self.assertEqual("v2", res["k2"])

    def test_parse_empty_list(self):
        in_list = []
        enricher = OneAgentMetadataEnricher()
        res = enricher._parse_oneagent_metadata(in_list)

        self.assertEqual(0, len(res))


class TestGetIndirectionFile(unittest.TestCase):
    def test_get_indirection_file_contents_success(self):
        # when open is called, a file with this content is returned
        mock = mock_open(read_data="metadata_file_name")
        enricher = OneAgentMetadataEnricher()
        with patch("builtins.open", mock):
            res = enricher._get_metadata_file_name("not-used-since-mocking")
            self.assertEqual("metadata_file_name", res)

    def test_get_indirection_file_missing_or_not_readable(self):
        # will throw an IOError when calling open
        mock = mock_open()
        mock.side_effect = IOError()
        enricher = OneAgentMetadataEnricher()
        with patch("builtins.open", mock):
            res = enricher._get_metadata_file_name("not-used-since-mocking")
            self.assertEqual(None, res)

    def test_get_indirection_fname_missing_empty_or_invalid(self):
        enricher = OneAgentMetadataEnricher()
        self.assertEqual(None, enricher._get_metadata_file_name(""))
        self.assertEqual(None, enricher._get_metadata_file_name(None))
        self.assertEqual(None, enricher._get_metadata_file_name("#%&^:"))


class TestGetMetadataContents(unittest.TestCase):
    def test_indirection_file_empty(self):
        mock = mock_open(read_data="")
        enricher = OneAgentMetadataEnricher()

        with patch("builtins.open", mock):
            res = enricher._get_metadata_file_content()
            self.assertEqual(0, len(res))

    def test_get_file_contents_success(self):
        # this only tests _get_metadata_file_content() paritally since both
        # open calls are mocked, so even if the file does not exist this test
        # will pass since the second open call is also mocked.
        mock = mock_open()
        # return "indirection_filename" on the first open call and the
        # key-vlaue pairs on the second.
        mock.side_effect = [mock_open(read_data=x).return_value for x in
                            ["indirection_filename",
                             "key1=value1\nkey2=value2"]]

        enricher = OneAgentMetadataEnricher()
        with patch("builtins.open", mock):
            res = enricher._get_metadata_file_content()
            self.assertEqual(2, len(res))
            # at this stage, values are not yet parsed. removing trailing 
            # whitespace is done in the _parse_oneagent_metadata function.
            self.assertEqual("key1=value1\n", res[0])
            self.assertEqual("key2=value2", res[1])

    @patch('dynatrace.opentelemetry.metrics.export.oneagentmetadataenricher'
           '.OneAgentMetadataEnricher._get_metadata_file_name')
    def test_get_file_contents_from_none(self, mock_func):
        mock_func.return_value = None

        enricher = OneAgentMetadataEnricher()
        res = enricher._get_metadata_file_content()
        self.assertEqual(0, len(res))

    @patch('dynatrace.opentelemetry.metrics.export.oneagentmetadataenricher'
           '.OneAgentMetadataEnricher._get_metadata_file_name')
    def test_get_file_contents_from_tmpfile(self, mock_func):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmpfile_name = os.path.join(tmp_dir, "tmp_file")
            with open(tmpfile_name, "w") as tmp_file:
                tmp_file.write("\n".join(["key1=value1", "key2=value2"]))

            mock_func.return_value = tmpfile_name

            enricher = OneAgentMetadataEnricher()
            res = enricher._get_metadata_file_content()
            self.assertEqual(2, len(res))
            self.assertEqual("key1=value1\n", res[0])
            self.assertEqual("key2=value2", res[1])

    @patch('dynatrace.opentelemetry.metrics.export.oneagentmetadataenricher'
           '.OneAgentMetadataEnricher._get_metadata_file_name')
    def test_get_file_contents_from_nonexistent_file(self, mock_func):
        temp = tempfile.NamedTemporaryFile()
        mock_func.return_value = temp.name
        # this will automatically delete the tempfile, making sure that it does
        # not exist any more
        temp.close()

        enricher = OneAgentMetadataEnricher()
        # will try to read from the not-anymore-existing tempfile
        res = enricher._get_metadata_file_content()
        self.assertEqual(0, len(res))

    @patch('dynatrace.opentelemetry.metrics.export.oneagentmetadataenricher'
           '.OneAgentMetadataEnricher._get_metadata_file_name')
    def test_get_file_contents_from_invalid_filename(self, mock_func):
        mock_func.return_value = "^%#&"

        enricher = OneAgentMetadataEnricher()
        res = enricher._get_metadata_file_content()
        self.assertEqual(0, len(res))
