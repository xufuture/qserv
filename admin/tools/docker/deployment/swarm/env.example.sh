# Rename this file to env.sh and edit variables
# Configuration file sourced by other scripts from the directory

# VERSION can be a git ticket branch but with _ instead of /
# example: u_fjammes_DM-4295
VERSION=dev

NB_WORKERS=2

# Swarm socker
export DOCKER_HOST="tcp://lsst-fabrice.jammes-qserv-swarm:3376"

# Set nodes names
HOSTNAME_TPL="lsst-fabrice.jammes-qserv-"
MASTER="${HOSTNAME_TPL}0"

for i in $(seq 1 "$NB_WORKERS");
do
    WORKERS="$WORKERS ${HOSTNAME_TPL}${i}"
done

# Set images names
MASTER_IMAGE="qserv/qserv:${VERSION}_master"
WORKER_IMAGE="qserv/qserv:${VERSION}_worker"


