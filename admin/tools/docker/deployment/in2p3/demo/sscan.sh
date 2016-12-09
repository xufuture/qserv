#!/bin/bash

# Run SQL queries against Qserv cluster
# See https://confluence.lsstcorp.org/display/DM/S15+Large+Scale+Tests
# @author Fabrice Jammes IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "$DIR/env.sh"

export MASTER

echo "SHARED SCANS"
echo "------------"

echo "Two scans on Object. Startup was staggered." 
echo
SQL1="SELECT COUNT(*) FROM Object WHERE y_instFlux > 5"
SQL2="SELECT MIN(ra), MAX(ra) FROM Object WHERE decl > 3"
parallel --delay 5 mysql_query ::: "$SQL1" "$SQL2"

echo "Five scans on Object. Startup was staggered."
echo
SQL1="SELECT MIN(ra), MAX(ra) FROM Object WHERE decl > 3"
SQL3="SELECT COUNT(*) FROM Object WHERE y_instFlux > u_instFlux"
SQL4="SELECT COUNT(*) FROM Object WHERE y_instFlux > 5"
SQL5="SELECT MIN(ra), MAX(ra) FROM Object WHERE z_apFlux BETWEEN 1 and 2"
parallel --delay 5 mysql_query ::: "$SQL1" "$SQL1" "$SQL3" "$SQL4" "$SQL5"

echo "Five scans on Object, without staggering."
echo
SQL1="SELECT COUNT(*) FROM Object WHERE y_instFlux > 5"
SQL2="SELECT MIN(ra), MAX(ra) FROM Object WHERE decl > 3"
SQL5="SELECT COUNT(*) AS n, AVG(ra), AVG(decl), chunkId FROM Object GROUP BY chunkId"
parallel --delay 5 mysql_query ::: "$SQL1" "$SQL2" "$SQL1" "$SQL1" "$SQL5"

echo "Five scans: four on Object, one on Source"
echo
SQL1="SELECT MIN(ra), MAX(ra) FROM Object WHERE decl > 3"
SQL2="SELECT COUNT(*) FROM Object WHERE y_instFlux > 5"
SQL5="SELECT COUNT(*) FROM Source WHERE flux_sinc BETWEEN 1 AND 2"
parallel mysql_query ::: "$SQL1" "$SQL2" "$SQL2" "$SQL2" "$SQL5"

