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

    def __init__(self, logger: logging.Logger) -> None:
        self.__logger = logger

    def add_oneagent_metadata_to_tags(self, tags: Mapping[str, str]):
        for key, value in self._parse_oneagent_metadata(self.__get_metadata_file_content()).items():
            tags[key] = value

    def __get_metadata_file_content(self) -> List[str]:
        try:
            metadata_file_name = None
            with open("dt_metadata_e617c525669e072eebe3d0f08212e8f2.properties", "r") as magic_file:
                metadata_file_name = magic_file.read()

            if not metadata_file_name:
                self.__logger.debug("metadata file name not specified by OneAgent.")
                return []

            with open(metadata_file_name, "r") as attributes_file:
                return attributes_file.readlines()

        except OSError:
            logging.warning("could not read OneAgent metadata file. This is normal if OneAgent is not installed.")

    def _parse_oneagent_metadata(self, lines) -> Mapping[str, str]:
        key_value_pairs = {}
        for line in lines:
            self.__logger.debug(f"parsing line {line}")

            split = line.strip().split("=", 1)

            if len(split) != 2:
                self.__logger.warning(f"could not parse line {line}")
                continue

            key, value = split

            # None or empty:
            if not key or not value:
                self.__logger.warning(f"could not parse line {line}")
                continue

            key_value_pairs[key] = value

        return key_value_pairs
