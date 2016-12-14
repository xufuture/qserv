#!/bin/bash

# Count objects on one Qserv worker

# Launch using parallel
# echo /tmp/count.sh | parallel --onall --slf qserv.slf "sh -c '{}'"

set -e

DB="LSST"

CONTAINER="qserv"

USER="qsmaster"
SOCKET="/qserv/run/var/lib/mysql/mysql.sock"

echo "node: $(hostname)"

SQL="SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA='$DB' \
     AND TABLE_NAME LIKE 'Object\_%' AND TABLE_ROWS>0"
TABLES=$(docker exec $CONTAINER \
	bash -c ". /qserv/stack/loadLSST.bash && \
	setup mariadb && \
	mysql -N -B --socket $SOCKET \
	--user=$USER $DB -e \"$SQL\"")
echo "tables: \"$TABLES\""

i=0
for t in $TABLES
do
    if [ $i -eq 0 ]; then
        SQL="SELECT SUM(o.c) FROM (select count(*) as c FROM $t "
    else
        SQL="$SQL UNION ALL select count(*) as c FROM $t"
    fi
    i=$((i+1))
done
SQL="${SQL}) o;"

RESULTS=$(docker exec "$CONTAINER" \
	bash -c ". /qserv/stack/loadLSST.bash && \
	setup mariadb && \
	mysql -N -B --socket $SOCKET \
	--user=$USER $DB -e \"$SQL"\")
echo "result: \"$RESULTS\""

#for t in $TABLES
#do
#    SQL="SELECT TABLE_NAME, TABLE_ROWS-(select count(*) as c FROM $t) FROM information_schema.tables WHERE \
#    TABLE_SCHEMA='$DB' AND TABLE_NAME='$t'"
#    RESULTS=$(docker exec "$CONTAINER" \
#	bash -c ". /qserv/stack/loadLSST.bash && \
#	setup mariadb && \
#	mysql -N -B --socket $SOCKET \
#	--user=$USER $DB -e \"$SQL"\")
#    echo "result_diff: \"$RESULTS\""
#done
