# cf https://dev.lsstcorp.org/cgit/LSST/DMS/qserv.git/tree/meta/examples/permissions.sql

DROP DATABASE IF EXISTS `qms_%(QMS_DB)s`;
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, INDEX, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE ON  `qms\_%`.* TO '%(QMS_USER)s'@'localhost' IDENTIFIED BY '%(QMS_PASS)s';
FLUSH PRIVILEGES;
