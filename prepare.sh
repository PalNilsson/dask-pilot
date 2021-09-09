#!/bin/bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2021

# Note: this file must be executable

set -x

# We start by adding extra apt packages, since pip modules may required library
#if [ "$EXTRA_APT_PACKAGES" ]; then
#    echo "EXTRA_APT_PACKAGES environment variable found.  Installing."
#    apt update -y
#    apt install -y $EXTRA_APT_PACKAGES
#fi

if [ -e "/opt/app/environment.yml" ]; then
    echo "environment.yml found. Installing packages"
    /opt/conda/bin/conda env update -f /opt/app/environment.yml
    conda init bash
    activate myenv
else
    echo "no environment.yml"
fi
whoami

#if [ "$EXTRA_CONDA_PACKAGES" ]; then
#    echo "EXTRA_CONDA_PACKAGES environment variable found.  Installing."
#    /opt/conda/bin/conda install -y $EXTRA_CONDA_PACKAGES
#fi

#if [ "$EXTRA_PIP_PACKAGES" ]; then
#    echo "EXTRA_PIP_PACKAGES environment variable found.  Installing".
#    /opt/conda/bin/pip install $EXTRA_PIP_PACKAGES
#fi

# Run extra commands
#exec "$@"
