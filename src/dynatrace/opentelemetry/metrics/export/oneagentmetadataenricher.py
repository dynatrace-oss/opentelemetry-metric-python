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
from typing import List, Mapping


class OneAgentMetadataEnricher:
    __logger = None

    def __init__(self) -> None:
        self.__logger = logging.Logger(self.__class__.__name__)

    def add_oneagent_metadata_to_tags(self, tags: Mapping[str, str]):
        metadata_file_content = self._get_metadata_file_content()
        parsed_metadata = self._parse_oneagent_metadata(metadata_file_content)
        for key, value in parsed_metadata.items():
            tags[key] = value

    def _get_metadata_file_name(self, indirection_fname: str) -> str:
        file_name = None
        if not indirection_fname:
            return None

        try:
            with open(indirection_fname, "r") as metadata_indirection_file:
                file_name = metadata_indirection_file.read()

        except OSError:
            self.__logger.info("Could not read local OneAgent indirection "
                               "file. This is normal if no OneAgent is "
                               "installed.")
        return file_name

    def _get_metadata_file_content(self) -> List[str]:
        try:
            metadata_file_name = self._get_metadata_file_name(
                "dt_metadata_e617c525669e072eebe3d0f08212e8f2.properties"
            )

            if not metadata_file_name:
                self.__logger.warning("Metadata file not specified in "
                                      "indirection file!")
                return []

            with open(metadata_file_name, "r") as attributes_file:
                return attributes_file.readlines()
        except OSError:
            self.__logger.info(
                "Could not read OneAgent metadata file. ({})".format(
                    metadata_file_name))
            return []

    def _parse_oneagent_metadata(self, lines) -> Mapping[str, str]:
        key_value_pairs = {}
        for line in lines:
            self.__logger.debug("Parsing line {}".format(line))

            # remove leading and trailing whitespace and split at the first '='
            split = line.strip().split("=", 1)

            if len(split) != 2:
                self.__logger.warning("Could not parse line {}".format(line))
                continue

            key, value = split

            # None or empty:
            if not key or not value:
                self.__logger.warning("Could not parse line {}".format(line))
                continue

            key_value_pairs[key] = value

        return key_value_pairs
