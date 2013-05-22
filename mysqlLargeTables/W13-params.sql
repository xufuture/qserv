set global myisam_repair_threads=8;                               -- # keys, higher number is ok
-- set global myisam_max_sort_file_size = 2G;              -- too large will fool the optimizer
set global myisam_max_sort_file_size = 3072*1024*1024*1024; -- ~2-3x MYD
-- plus, enough space in tmpdir to hold myisam_max_sort_file_size of data

GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, INDEX, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE ON  `w13`.* TO 'w13'@'%' IDENTIFIED BY 'changeme';
FLUSH PRIVILEGES;
