seq --format 'ccqserv%3.0f' 100 124 | /sps/lsst/data/gapon/tools/bin/shmux -c "source /sps/lsst/data/gapon/stack/loadLSST.bash; setup -t qserv-dev \
mariadbclient; echo 'SELECT SUM(TABLE_ROWS) FROM information_schema.tables \
WHERE TABLE_SCHEMA=\""LSST\"" AND TABLE_NAME LIKE \""Object\\_%\"" AND \
TABLE_ROWS>0' |  mysql -A -N -B -S /qserv/data/mysql/mysql.sock -h localhost \
-P13306 -uqsmaster LSST" - | \
grep 'ccqserv' | awk 'BEGIN {total=0} {total += $2} END {print total}'
