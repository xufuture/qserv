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

echo "SHARED SCANS"
echo "------------"
# TODO
#Two scans on Object, both finished in ~8.5 min or so. Startup was staggered.
#
#QTYPE_FTSObj: 505.703582048  SELECT COUNT(*) FROM Object WHERE y_instFlux > 5
#QTYPE_FTSObj: 505.837508917  SELECT MIN(ra), MAX(ra) FROM Object WHERE decl >
#3
#
#Five scans on Object finished in 16-20 min. Startup was staggered.
#
#QTYPE_FTSObj: 990.450098038 SELECT MIN(ra), MAX(ra) FROM Object WHERE decl > 3
#QTYPE_FTSObj: 1168.69941115 SELECT MIN(ra), MAX(ra) FROM Object WHERE decl > 3
#QTYPE_FTSObj: 1180.72830892 SELECT COUNT(*) FROM Object WHERE y_instFlux >
#u_instFlux
#QTYPE_FTSObj: 1178.19018197 SELECT COUNT(*) FROM Object WHERE y_instFlux > 5
#QTYPE_FTSObj: 1173.29835892 SELECT MIN(ra), MAX(ra) FROM Object WHERE z_apFlux
#BETWEEN 1 and 2
#
#Five scans on Object, without staggering, not much difference:
#
#QTYPE_FTSObj: 738.438729763 SELECT COUNT(*) FROM Object WHERE y_instFlux > 5
#QTYPE_FTSObj: 1162.67162609 left 2437.32837391 SELECT MIN(ra), MAX(ra) FROM
#Object WHERE decl > 3
#QTYPE_FTSObj: 1169.67710209 left 2430.32289791 SELECT COUNT(*) FROM Object
#WHERE y_instFlux > 5
#QTYPE_FTSObj: 1171.61784506 left 2428.38215494 SELECT COUNT(*) FROM Object
#WHERE y_instFlux > 5
#QTYPE_FTSObj: 1171.95623493 left 2428.04376507 SELECT COUNT(*) AS n, AVG(ra),
#AVG(decl), chunkId FROM Object GROUP BY chunkId
#
#Five scans: four on Object, one on Source: ~1h10 min per scan
#
#QTYPE_FTSObj: 4237.70917988 SELECT MIN(ra), MAX(ra) FROM Object WHERE decl > 3
#QTYPE_FTSObj: 4262.98238802 SELECT COUNT(*) FROM Object WHERE y_instFlux > 5
#QTYPE_FTSObj: 4263.39259911SELECT COUNT(*) FROM Object WHERE y_instFlux > 5
#QTYPE_FTSObj: 4263.39338088 SELECT COUNT(*) FROM Object WHERE y_instFlux > 5
#QTYPE_FTSSrc: 4264.03135395 SELECT COUNT(*) FROM Source WHERE flux_sinc
#BETWEEN 1 AND 2
#
