. ./env.sh

shmux -c "docker pull fjammes/qserv:master-${XRD_MASTER}" $MASTER 
shmux -c "docker pull fjammes/qserv:worker-${XRD_MASTER}" $WORKERS 
