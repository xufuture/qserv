#!/bin/bash

/usr/local/mysql/bin/mysql -u root < ~/W13-db.sql
/usr/local/mysql/bin/mysql -u root w13 </data/LSST/Winter2013/RunDeepForcedSource.sql

function run 
{
    echo $1; 
    eval "$1" 2>&1 | sed 's/^/stdout and stderr : /';
} 

CMD="time /usr/local/mysql/bin/mysql -u root -e \"ALTER TABLE RunDeepForcedSource DISABLE KEYS\" w13"; 
run "${CMD}"; 

DATA=`find /data/LSST/Winter2013/RunDeepForcedSource/ -type f`;
for f in $DATA; 
do 
CMD="time /usr/local/mysql/bin/mysql -u root -e \"LOAD DATA INFILE '$f' INTO TABLE RunDeepForcedSource\" w13"; 
run "${CMD}"; 
done

#CMD="time /usr/local/mysql/bin/mysql -u root -e \"ALTER TABLE RunDeepForcedSource ENABLE KEYS\" w13"; 
#run "${CMD}"; 
