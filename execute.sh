#!/bin/bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

# Note: this file must be executable

echo "starting pilotx"
whoami
pwd
which rucio
echo $RUCIO_HOME
# /opt/conda/bin/ has rucio binary
cp /opt/conda/etc/rucio.cfg.template /home/dask/rucio/etc/rucio.cfg
ls -lF /opt/conda/bin
export $PYTHONPATH=/opt/conda/lib/
rucio download
#python3 -V
#python3 /user/share/pilot3/pilot.py
echo "finished pilotx"
