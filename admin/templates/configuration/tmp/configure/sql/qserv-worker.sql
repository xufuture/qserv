-- Used by xrootd Qserv plugin:

--   to publish LSST databases
DROP DATABASE IF EXISTS qservw_worker;
CREATE DATABASE qservw_worker;
GRANT ALL ON qservw_worker.* TO 'qsmaster'@'localhost';
CREATE TABLE qservw_worker.Dbs (
  `db` char(200) NOT NULL
);

CREATE DATABASE IF NOT EXISTS qservScratch;
GRANT ALL ON qservScratch.* TO 'qsmaster'@'localhost';
GRANT SELECT ON mysql.* TO 'qsmaster'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, INDEX, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE ON `q\_%`.* TO 'qsmaster'@'localhost' WITH GRANT OPTION;

-- Subchunks databases
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, INDEX, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE ON `Subchunks\_%`.* TO 'qsmaster'@'localhost' WITH GRANT OPTION;

-- Database for business (i.e. LSST) data 
-- Has to be created by the dataloader in the long term
CREATE DATABASE IF NOT EXISTS LSST;
GRANT ALL ON LSST.* TO 'qsmaster'@'localhost';
GRANT EXECUTE ON `LSST`.* TO 'qsmaster'@'localhost';
INSERT INTO qservw_worker.Dbs VALUES('LSST'); 

