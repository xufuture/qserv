set -e
set -x

QSERV_RUN_DIR=~/qserv-run/2014_11.0/

killall mysqld xrootd mysql-proxy java python || echo "Nothing to kill" 
scons install
qserv-configure.py --all --force

# hack which moves qservMeta to czar DB
# indeed this table in created by mono-node
# integration test alongside MySQL data
${QSERV_RUN_DIR}/bin/qserv-start.sh
qserv-check-integration.py --load --case=01 || echo "Failed but OK"
${QSERV_RUN_DIR}/bin/qserv-stop.sh
cp -r ${QSERV_RUN_DIR}/var/lib/mysql-worker/qservMeta/ ${QSERV_RUN_DIR}/var/lib/mysql/
# Remove qservMeta on the worker
rm -rf  ${QSERV_RUN_DIR}/var/log/*
mysqld_safe --defaults-file=${QSERV_RUN_DIR}/etc/my.worker.cnf >/dev/null 2>&1 &
sleep 2
mysql --no-defaults --socket="${QSERV_RUN_DIR}/var/lib/mysql-worker/mysql.sock" --user=root -pchangeme -e "DROP DATABASE qservMeta"
kill `cat ${QSERV_RUN_DIR}/var/run/mysqld-worker/mysqld.pid`

~/qserv-run/2014_11.0/bin/qserv-start.sh
qserv-check-integration.py --case=01
