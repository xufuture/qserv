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
mysql_query "$SQL"

SQL="SELECT count(*) FROM Source"
mysql_query "$SQL"

SQL="SELECT count(*) FROM ForcedSource"
mysql_query "$SQL"

