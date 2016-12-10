#!/bin/bash

# Run SQL queries against Qserv cluster
# See https://confluence.lsstcorp.org/display/DM/S15+Large+Scale+Tests
# @author Fabrice Jammes IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

echo "COUNTS"
echo "------"
echo
SQL="SELECT count(*) FROM Object"
echo "$SQL"
time mysql --host "$MASTER" --port 4040 --user qsmaster LSST -e "$SQL"
echo

SQL="SELECT count(*) FROM Source"
echo "$SQL"
time mysql --host "$MASTER" --port 4040 --user qsmaster LSST -e "$SQL"
echo

SQL="SELECT count(*) FROM ForcedSource"
echo "$SQL"
time mysql --host "$MASTER" --port 4040 --user qsmaster LSST -e "$SQL"
echo

