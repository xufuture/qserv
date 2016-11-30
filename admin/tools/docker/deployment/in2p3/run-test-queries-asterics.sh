#!/bin/bash

# Run SQL queries against Qserv cluster
# See https://confluence.lsstcorp.org/display/DM/S15+Large+Scale+Tests
# @author Fabrice Jammes IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

. /qserv/stack/loadLSST.bash

setup mariadbclient
#time mysql --host "$MASTER" --port 4040 --user qsmaster LSST -e "SHOW tables"

echo "Trivial query that retrieves one row, using index"
echo "-------------------------------------------------"
SQL="SELECT ra, decl FROM Object WHERE deepSourceId = 2322920177142607;"
echo "$SQL"
time mysql --host "$MASTER" --port 4040 --user qsmaster LSST -e "$SQL"
echo

echo "Counts"
echo "------"
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

echo "Spatially restricted query, small area of sky, should return small \
    number of rows (say <100)"

SQL="SELECT COUNT( * ) FROM Object WHERE ra_PS BETWEEN 1 AND 2 AND decl_PS BETWEEN 3 AND 4"
echo "$SQL"
time mysql --host "$MASTER" --port 4040 --user qsmaster LSST -e "$SQL"

echo "Full table scan, use some column in WHERE that is not indexes, make \
sure the number of results returned is sane (eg thousands, not millions)"

SQL="SELECT objectId, ra_PS, decl_PS, <few other columns> \
FROM Object \
WHERE fluxToAbMag(iFlux_PS) - fluxToAbMag(zFlux_PS) > 4"

echo "Aggregation"

SQL="SELECT COUNT(*) AS n, \
    AVG(ra_PS), \
    AVG(decl_PS), chunkId \
    FROM Object \
    GROUP BY chunkId"

echo "Near neighbor"

SQL="SELECT COUNT(*) \
    FROM Object o1, Object o2 \
    WHERE qserv_areaspec_box(-5,-5,5,-5) \
    AND qserv_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 0.1"

echo "Joins"

SQL="SELECT o.objectId, s.sourceId, ra_PS, decl_PS, <few other columns>
    FROM Object
    JOIN SOURCE USING (objectId)
    WHERE fluxToAbMag(iFlux_PS) - fluxToAbMag(zFlux_PS) > 4
    AND <some restriction from source table>"
