cd ${DIR}/..
. ./env.sh
cd -

. /qserv/stack/loadLSST.bash
setup mariadbclient

function mysql_query {
    SQL="$1"
    echo "mysql --host $MASTER --port 4040 --user qsmaster LSST -e \"$SQL\""
    time mysql --host "$MASTER" --port 4040 --user qsmaster LSST -e "$SQL"
    echo
    if [ -n "$2" ]; then
        sleep $2
    fi
}  
