#!/bin/bash

python mr_join.py hdfs:///user/ad3254_nyu_edu/3635813.csv hdfs:///user/ad3254_nyu_edu/lax_to_jfk.csv -r hadoop \
       --output-dir mr_join \
       --python-bin /opt/conda/default/bin/python
