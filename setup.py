# Copyright 2020 Dynatrace LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
import setuptools

setuptools.setup(
    name="dynatrace-opentelemetry-metrics-export",
    install_requires=("opentelemetry-api==0.15b0","opentelemetry-sdk==0.15b0"," requests~=2.25")
)
