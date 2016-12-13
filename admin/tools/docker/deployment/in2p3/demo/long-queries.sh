#!/bin/bash

# Run SQL queries against Qserv cluster
# See https://confluence.lsstcorp.org/display/DM/S15+Large+Scale+Tests
# @author Fabrice Jammes IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

echo "FULL TABLE SCANS"
echo "----------------"

echo "Full table scan, use some column in WHERE that is not indexed, make \
sure the number of results returned is sane (eg thousands, not millions)"
echo 
SQL="SELECT COUNT(*)  FROM Object WHERE y_instFlux > 5"
mysql_query "$SQL"

SQL="select min(ra), max(ra), min(decl), max(decl) from Object;"
mysql_query "$SQL"

SQL="select count(*) from Source where flux_sinc between 1 and 2;"
mysql_query "$SQL"

SQL="select count(*) from Source where flux_sinc between 2 and 3;"
mysql_query "$SQL"

SQL="select count(*) from ForcedSource where psfFlux between 0.1 and 0.2;"
mysql_query "$SQL"

echo "JOINS"
echo "-----"

SQL="select count(*) from Object o, Source s WHERE o.deepSourceId=s.objectId \
AND s.flux_sinc BETWEEN 0.13 AND 0.14;"
mysql_query "$SQL"

SQL="select (*) FROM Object o, ForcedSource f WHERE \
o.deepSourceId=f.deepSourceId AND f.psfFlux BETWEEN 0.13 AND 0.14;"
mysql_query "$SQL"

