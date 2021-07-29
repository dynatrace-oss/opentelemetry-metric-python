#!/bin/bash

# check if running as root. Helpful in environments where you are root but sudo is not installed. 
if [ $EUID -eq 0 ]; then 
	apt-get update 
	apt-get install -y python3 python3-pip python3-venv
else
	sudo apt-get update 
	sudo apt-get install -y python3 python3-pip python3-venv
fi

# change into the opentelemetry-metric-python folder if you haven't already
python3 -m venv .venv   `# create a new virtual environment in the current folder`

source .venv/bin/activate
pip3 install --upgrade setuptools   `# make sure setuptools and wheel are on the latest version`
pip3 install psutil                 `# for observing cpu and ram`
pip3 install .                      `# install the library itself`
# Valid log levels are: DEBUG, INFO, WARN/WARNING, ERROR, CRITICAL/FATAL
export LOGLEVEL=DEBUG               `# set the log level`
python3 example/basic_example.py    `# run the example in a venv`
