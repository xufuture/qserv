#!/bin/bash

# Set up defaults
host=localhost
dbname=lsst
username=lsst
password=lsst
port=50000

monetdb_login="-p$port"

# Add monetdb to path
TOP=/scratch/danielw
MONETPATH=/scratch/bscheers/MonetDB-Apr2012-SP1/bin
export PATH=$MONETPATH:$PATH

echo "(re)creating $dbname at $host"
monetdb $monetdb_login stop $dbname
monetdb $monetdb_login destroy -f $dbname
monetdb $monetdb_login create $dbname #|| exit 1
monetdb $monetdb_login start $dbname #|| exit 1
monetdb $monetdb_login release $dbname #|| exit 1
    
adminuser=$username
adminpassword=$password
    
DEFAULTDOTFILE=.monetdb
DOTMONETDBFILE=$HOME/$DEFAULTDOTFILE

# set up a default .monetdb file, with default pass
cat > $DOTMONETDBFILE <<EOF
user=monetdb
password=monetdb
EOF
    
echo "changing monetdb/monetdb user/password into:"
echo "user: ${adminuser}"
echo "password: ${adminpassword}"
mclient -h$host -p$port -d$dbname <<-EOF
ALTER USER "monetdb" RENAME TO "${adminuser}";
ALTER USER SET PASSWORD '${adminpassword}' USING OLD PASSWORD 'monetdb';
CREATE SCHEMA "${dbname}" AUTHORIZATION "${adminuser}";
ALTER USER "${adminuser}" SET SCHEMA "${dbname}";
EOF
    
# Here we set the DOTMONETDBFILE to the credentials of the current dbname
DOTMONETDBFILE=$HOME/.${dbname}
cat > $DOTMONETDBFILE <<EOF
user=${adminuser}
password=${adminpassword}
EOF
chmod go-rwx $DOTMONETDBFILE
export DOTMONETDBFILE

lsstdir=/scratch/danielw/lsst

echo -e "\t\tCreating MonetDB ${dbname} table object"
mclient -p$port -h$host -d${dbname} < ${lsstdir}/create.table.object.sql || exit 1
LOADFILE=load.object.tmp.sql

>$LOADFILE

chunkdir=$lsstdir/chunks
for f in $chunkdir/*csv; do
    fname=${f##${chunkdir}/}
    tname=${fname%%.csv}
    echo -e "\t\tCreating MonetDB ${dbname} table $tname"
    time mclient -p$port -h$host -d${dbname} -s "create table $tname (like object);" || exit 1
    echo -e "\t\tMonetDB ${dbname}, COPY INTO TABLE $tname"
    sqlcmd="COPY 4200000 RECORDS INTO $tname FROM '$chunkdir/$fname' USING DELIMITERS ',' ,'\\n' NULL AS '\\\\N';"
    echo $sqlcmd >>$LOADFILE
done

echo Trying `cat $LOADFILE`
time mclient -p$port -h$host -d${dbname} < $LOADFILE

##
echo -e "-----"
echo -e "READY"
echo -e "-----"
