
import logging

from cssInterface import CssInterface, CssException

def getDbStriping(dbName):
    logging.basicConfig(
        #filename="/tmp/testCssInterface.log",
        format='%(asctime)s %(name)s %(levelname)s: %(message)s', 
        datefmt='%m/%d/%Y %I:%M:%S', 
        level=logging.DEBUG)
    logging.getLogger("kazoo.client").setLevel(logging.ERROR)
    cssI = CssInterface("127.0.0.1:2181")
    print "dumping all *****"
    cssI.dumpAll()
    print "done dumping all *****"
    pId = cssI.get("/DATABASES/"+dbName+"/partitioningId")
    nStripes = cssI.get("/DATABASE_PARTITIONING/_"+pId+"/nStripes")
    nSubStripes = cssI.get("/DATABASE_PARTITIONING/_"+pId+"/nSubStripes")
    return (int(nStripes), int(nSubStripes))
