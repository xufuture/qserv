#!/bin/bash

# Run SQL queries against Qserv cluster
# See https://confluence.lsstcorp.org/display/DM/S15+Large+Scale+Tests
# @author Fabrice Jammes IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "$DIR/env.sh"

echo "SHORT-RUNNING QUERIES"
echo "---------------------"

echo "Describe object table"
SQL="Describe Object"
mysql_query "$SQL" 3

echo "Trivial query that retrieves one row, using index"
echo
SQL="SELECT ra, decl, raVar, declVar, radeclCov, u_psfFlux, \
u_psfFluxSigma, u_apFlux FROM Object WHERE deepSourceId = 2322920177140010;"
mysql_query "$SQL" 1
SQL="SELECT ra, decl, raVar, declVar, radeclCov, u_psfFlux, \
u_psfFluxSigma, u_apFlux FROM Object WHERE deepSourceId = 2322920177142607;"
mysql_query "$SQL" 3

echo "Spatially restricted query, small area of sky, should return small \
number of rows (say <100)"
echo
SQL="SELECT ra, decl FROM Object WHERE qserv_areaspec_box(0.95, 19.171, 1.0, 19.175);"
mysql_query "$SQL" 3

echo "NEAR NEIGHBOR"
echo "-------------"

SQL="select count(*) \
from Object o1, Object o2 \
where qserv_areaspec_box(90.299197, -66.468216, 90.962526, -65.412851) and \
scisql_angSep(o1.ra, o1.decl, o2.ra, o2.decl) < 0.015;"
mysql_query "$SQL"

SQL="select o1.deepSourceId as id1, o2.deepSourceId as id2, o1.ra, o1.decl, \
scisql_angSep(o1.ra, o1.decl, o2.ra, o2.decl) as dist from Object o1, Object \
o2 where qserv_areaspec_box(89.299197, -66.468216, 89.312526, -66.452851) and \
scisql_angSep(o1.ra, o1.decl, o2.ra, o2.decl) < 0.015 ORDER BY id1;"
mysql_query "$SQL"

