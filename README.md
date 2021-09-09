# dask-pilot

This image creates a pilot that can be used for executing dask user scripts. It requires
a running dask scheduler and worker(s). The yaml file (not yet included here) must define the scheduler IP as an
env variable ($DASK_SCHEDULER_IP) as well as a shared directory ($DASK_SHARED_FILESYSTEM_PATH).

The image is based on the continuumio/miniconda3:4.8.2 base image.

## Build instructions

1. docker build -t dask-pilot:latest .
2. Identify the < build tag > at the bottom of the stdout from the previous step
3. docker tag < build tag > < docker username >/dask-pilot:latest
4. docker push < docker username >/dask-pilot:latest
