ALTER TABLE object
DROP CONSTRAINT "object_objectid_pkey"
;

COPY 4200000 RECORDS 
INTO object
FROM '/scratch/bscheers/lsst/Object.csv'
USING DELIMITERS ','
                ,'\n'
NULL AS '\\N'
;

ALTER TABLE object
ADD PRIMARY KEY (objectId)
;

