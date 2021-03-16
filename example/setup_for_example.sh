#!/bin/bash

sudo apt-get update 
sudo apt-get install -y python3 python3-pip
pip3 install --upgrade setuptools

# change into the opentelemetry-metric-python folder if you havent already
pip3 install . 
python3 example/basic_example.py
