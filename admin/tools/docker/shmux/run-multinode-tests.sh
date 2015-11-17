#!/bin/bash

# Launch Qserv multinode tests
# using Docker containers

set -x

. ./env.sh

IMAGE_MASTER="fjammes/qserv:latest_master_$XRD_MASTER"
IMAGE_WORKER="fjammes/qserv:latest_worker_$XRD_MASTER"

shmux -c "docker rm -f qserv; nohup docker run --net=host --name qserv --rm \
    -p 4040:4040 -p 1094:1094 -p 2131:2131 -p 12181:12181 -p 5012:5012 $IMAGE_MASTER \
    > qserv.out 2> qserv.err < /dev/null &" $MASTER

# hostname has to be evaluated remotely 
shmux -c 'docker rm -f qserv; nohup docker run --net=host --name qserv --rm \
    -p 1094:1094 -p 5012:5012 '$IMAGE_WORKER' \
    > qserv.out 2> qserv.err < /dev/null &' $WORKERS

shmux -c "docker exec qserv bash -c '. /qserv/stack/loadLSST.bash && setup qserv_distrib && \
    echo \"$(cat nodes.example.css)\" | qserv-admin.py && qserv-test-integration.py -V DEBUG'" $MASTER
