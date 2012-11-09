#!/bin/bash

# Set up defaults
host=localhost
dbname=lsst
username=lsst
password=lsst
port=50010

monetdb_login="-p$port"

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

lsstdir=/scratch/bscheers/lsst

echo -e "\t\tCreating MonetDB ${dbname} functions degrees & radians"
mclient -p$port -h$host -d${dbname} < ${lsstdir}/monetdb_10_math.sql || exit 1

echo -e "\t\tCreating MonetDB ${dbname} function alpha"
mclient -p$port -h$host -d${dbname} < ${lsstdir}/create.function.alpha.sql || exit 1

echo -e "\t\tCreating MonetDB ${dbname} table object"
mclient -p$port -h$host -d${dbname} < ${lsstdir}/create.table.object.sql || exit 1

echo -e "\t\tMonetDB ${dbname}, COPY INTO TABLE object (replicated over all nodes)"
time mclient -p$port -h$host -d${dbname} < ${lsstdir}/load.object.sql || exit 1

echo -e "\t\tMonetDB ${dbname}, ALTER TABLE object ADD COLUMNs zone,x,y,z (replicated over all nodes)"
time mclient -p$port -h$host -d${dbname} < ${lsstdir}/alter.table.object.update.sql || exit 1

echo -e "\t\tCreating MonetDB ${dbname} table source (original)"
mclient -p$port -h$host -d${dbname} < ${lsstdir}/create.table.source.sql || exit 1

echo -e "\t\tLoading MonetDB ${dbname} table source (original)"
time mclient -p$port -h$host -d${dbname} < ${lsstdir}/load.source.sql || exit 1

echo -e "\t\tAlter MonetDB ${dbname} table source and update"
time mclient -p$port -h$host -d${dbname} < ${lsstdir}/alter.table.source.update.sql || exit 1

echo -e "-----"
echo -e "READY"
echo -e "-----"
