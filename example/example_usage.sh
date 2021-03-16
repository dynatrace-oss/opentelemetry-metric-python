#!/bin/bash

# check if running as root
if [ $EUID -eq 0 ]; then 
	apt-get update 
	apt-get install -y python3 python3-pip python3-venv
else
	sudo apt-get update 
	sudo apt-get install -y python3 python3-pip python3-venv
fi

# change into the opentelemetry-metric-python folder if you havent already
python3 -m venv .venv

# this will NOT set the venv for the shell that is calling this if run with ./example/setup_for_example.sh
# to get this effect run source example/setup_for_example.sh
source .venv/bin/activate 
pip3 install --upgrade setuptools wheel
pip3 install . 
python3 example/basic_example.py
