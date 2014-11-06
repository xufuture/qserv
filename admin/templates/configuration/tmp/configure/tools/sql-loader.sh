
QSERV_DIR={{QSERV_DIR}}
QSERV_RUN_DIR={{QSERV_RUN_DIR}}
MYSQL_DIR={{MYSQL_DIR}}
MYSQLD_USER={{MYSQLD_USER}}

SQL_DIR=${QSERV_RUN_DIR}/tmp/configure/sql

MYSQL_CMD="${MYSQL_DIR}/bin/mysql --no-defaults -vvv --user=${MYSQLD_USER} --password=${MYSQLD_PASS} --sock=${MYSQLD_SOCK}"

${QSERV_RUN_DIR}/etc/init.d/mysqld start &&
mysqld_safe --defaults-file=${QSERV_RUN_DIR}/etc/my.worker.cnf &&
echo "-- Loading ${SQL_FILE} in MySQL"
${MYSQL_CMD} < ${SQL_DIR}/${SQL_FILE} && 
${QSERV_RUN_DIR}/etc/init.d/mysqld stop || 
{
    exit 1
    >&2 echo "ERROR: unable to load ${SQL_FILE}"
}
killall mysqld_safe || echo -n 
