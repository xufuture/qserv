#!/usr/bin/env python
# example: monetdump.py localhost 50000 monetdb monetdb voc emp dumpfile.txt
import os,sys
import subprocess
import tempfile
from textwrap import dedent
from itertools import ifilter

try:
    [mhost,mport,muser,mpassword,mdb,mtable, dumpfile] = sys.argv
except:
    print "No args spec'd. Using defaults."
    # test overrides
    mhost="localhost"
    mport="50000"
    muser="monetdb"
    mpassword="monetdb"
    mdb="voc"
    mschema="sys"
    mtable="emp"
    dumpfile="/tmp/mydump.sql"

#dumpProg="/scratch/danielw/MonetDB-Apr2012-SP1/bin/msqldump";
#cmd="$dumpProg --host=$mhost --port=$mport --user=$muser --describe --database $mdb"

mClient="/scratch/danielw/MonetDB-Apr2012-SP1/bin/mclient";
dumpSchemaTemplate="SELECT col.name, col.type FROM {schema}.tables AS tab, {schema}._columns AS col WHERE tab.name='{table}' AND tab.id = col.table_id ORDER BY col.number;"
dumpSchema = dumpSchemaTemplate.format(schema=mschema, table=mtable)
mClientCmd="$mClient --database $mdb --format=csv"

def makeDotFile(user, password):
    dotmonetdbfile = dedent("""
        user=%s
        password=%s
        """) % (user, password)
    f = tempfile.NamedTemporaryFile(delete=False)
    f.write(dotmonetdbfile)
    f.close()
    return f.name


def callMclient(dotfilename, stmt):
    os.environ["DOTMONETDBFILE"] = dotfilename
    cmdList = [mClient, "--database", mdb, "--format=csv", "-s", stmt]
    return subprocess.check_output(cmdList)

def startMclient(dotfilename, stmt):
    os.environ["DOTMONETDBFILE"] = dotfilename
    cmdList = [mClient, "--database", mdb, "--format=csv", "-s", stmt]
    p = subprocess.Popen(cmdList, stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
    for line in p.stdout:
        yield line
    p.stdout.close()


xmap = {
    "real" : "float",
    }
def remapType(monetType):
    try:
        return xmap[monetType]
    except:
        return monetType

def makeCreate(tablename, columns):
    prefix = "CREATE TABLE '%s' (" % tablename
    suffix = ");"
    core = ",\n".join(["'%s' %s" % (cname, remapType(ctype)) for (cname, ctype) in columns])
    statement = "\n".join([prefix,core,suffix])
    return statement

def makeDump(dotfile, tablename, targetfile, prepend):
    #cmd = "copy select * from %s INTO '%s' USING DELIMITERS ',', '\\n';" % (tablename, targetfile)
    cmd = "select * from %s;" % (tablename)
    target = open(targetfile, "w")
    target.write(prepend)
    for line in startMclient(dotfile, cmd):
        ins = "INSERT INTO %s VALUES (%s);\n"
        quoted = ",".join("'"+s+"'" for s in line.rstrip().split(","))
        target.write(ins % (tablename, quoted))
    target.close()

        
dotfile = makeDotFile(muser, mpassword)
columnTuples = callMclient(dotfile, dumpSchema).split("\n")
columns = [s.split(",") for s in columnTuples if s]
makeDump(dotfile, mtable, dumpfile, makeCreate(mtable, columns) + "\n")
os.unlink(dotfile)


#echo $dumpSchema | $mClientCmd | sed '1,$s|\(.*\),\(.*\)|\"\1\" \2|'
#CREATE TABLE "sys"."result_x" (
#"L1" BIGINT,
#"L2" DOUBLE
#);


# cleanup:
#rm $OUT
