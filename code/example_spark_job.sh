#!/bin/bash

# want to use this virtual environment
export PYSPARK_PYTHON=/Users/RobertSmith/PycharmProjects/experimentation/venv/bin/python

/Users/RobertSmith/spark-2.4.7-bin-hadoop2.7/bin/spark-submit \
--master local \
/Users/RobertSmith/spark-2.4.7-bin-hadoop2.7/examples/src/main/python/pi.py 10set