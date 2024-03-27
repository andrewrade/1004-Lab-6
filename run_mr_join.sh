#!/bin/bash

python mr_join.py ../*.csv -r hadoop \
       --output-dir mr_join \
       --python-bin /opt/conda/default/bin/python
