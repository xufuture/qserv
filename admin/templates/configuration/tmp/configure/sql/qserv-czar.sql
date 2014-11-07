CREATE DATABASE IF NOT EXISTS qservResult;
GRANT ALL ON qservResult.* TO 'qsmaster'@'localhost';

-- Secondary index database (i.e. objectId/chunkId relation)
-- created by integration test script/loader for now
GRANT SELECT ON qservMeta.* TO 'qsmaster'@'localhost';

-- Database for business (i.e. LSST) data 
-- In the long term:
--    * has to created by the dataloader
--    * should be only on workers
CREATE DATABASE IF NOT EXISTS LSST;
GRANT ALL ON LSST.* TO 'qsmaster'@'localhost';
GRANT EXECUTE ON `LSST`.* TO 'qsmaster'@'localhost';



