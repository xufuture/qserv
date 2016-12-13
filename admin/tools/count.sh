#!/bin/bash

# Count objects on one Qserv worker

# Launch using parallel
# echo /tmp/count.sh | parallel --onall --slf qserv.slf "sh -c '{}'"

set -e

DB="qservTest_case01_qserv"

CONTAINER="worker1.localdomain"

SQL1="show tables like 'Object\_%'"
TABLES=$(docker exec $CONTAINER \
	bash -c ". /qserv/stack/loadLSST.bash && \
	setup mariadb && \
	mysql -N -B --socket /qserv/run/var/lib/mysql/mysql.sock \
	--user=root --password=CHANGEME $DB -e \"$SQL1\"")

i=0
for t in $TABLES
do
    if [ $i -eq 0 ]; then
        SQL="SELECT SUM(o.c) FROM (select count(*) as c FROM $t "
    else
        SQL="$SQL UNION select count(*) as c FROM $t"
    fi
    i=$((i+1))
done
SQL="${SQL}) as o;"

docker exec "$CONTAINER" \
	bash -c ". /qserv/stack/loadLSST.bash && \
	setup mariadb && \
	mysql -N -B --socket /qserv/run/var/lib/mysql/mysql.sock \
	--user=root --password=CHANGEME '$DB' -e '$SQL'"
