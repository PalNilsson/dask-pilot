#!/bin/bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2021

# Note: this file must be executable

echo "starting pilotx"
whoami
pwd
python3 /user/share/pilot-code/pilot.py
echo "finished pilotx"
