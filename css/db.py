#!/usr/bin/env python

# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.

"""
This module is a wrapper around MySQLdb. It contains a set of low level basic
database utilities such as connecting to database. It caches connections, and
handles database errors. It is currently used only by the qserv metadata server,
but is could be easily turned into a generic module and used outside of metadata
code.
"""

from __future__ import with_statement
import MySQLdb as sql
import _mysql_exceptions
import MySQLdb
import StringIO
import subprocess
from datetime import datetime
from time import sleep


####################################################################################
#### DbStatus class. Defines erorr codes and messages used by the Db class.
####################################################################################
class DbStatus:
    # note: error numbered 1000 - 1200 are used by mysql,
    # see mysqld_ername.h in mysql source code
    SUCCESS                =    0
    ERR_CANT_CONNECT_TO_DB = 1500
    ERR_CANT_EXEC_SCRIPT   = 1505
    ERR_DB_EXISTS          = 1510
    ERR_DB_DOES_NOT_EXIST  = 1515
    ERR_INVALID_DB_NAME    = 1520
    ERR_MISSING_CON_INFO   = 1525
    ERR_MYSQL_CONNECT      = 1530
    ERR_MYSQL_DISCONN      = 1535
    ERR_MYSQL_ERROR        = 1540
    ERR_NO_DB_SELECTED     = 1545
    ERR_NOT_CONNECTED      = 1550
    ERR_INTERNAL           = 9999

    errors = {
        ERR_CANT_CONNECT_TO_DB: ("Can't connect to database."),
        ERR_CANT_EXEC_SCRIPT: ("Can't execute script."),
        ERR_DB_EXISTS: ("The database already exists."),
        ERR_DB_DOES_NOT_EXIST: ("The database does not exist."),
        ERR_INVALID_DB_NAME: ("Invalid database name."),
        ERR_MISSING_CON_INFO: ("Missing connection information -- must provide "
                               "either host/port or socket."),
        ERR_MYSQL_CONNECT: ("Unable to connect to mysql server."),
        ERR_MYSQL_DISCONN: ("Failed to commit transaction and "
                            "disconnect from mysql server."),
        ERR_MYSQL_ERROR: ("Internal MySQL error."),
        ERR_NO_DB_SELECTED: ("No database selected."),
        ERR_NOT_CONNECTED: ("Not connected to MySQL."),
        ERR_INTERNAL: ("Internal error.")
    }


####################################################################################
#### DbException class. Exceptions raised by the Db class.
####################################################################################
class DbException(Exception):
    def __init__(self, errNo, extraMsg1=None, extraMsg2=None):
        self._errNo = errNo
        self._extraMsg1 = extraMsg1
        self._extraMsg2 = extraMsg2

    def getErrMsg(self):
        msg = ''
        s = DbStatus()
        if self._errNo in s.errors: msg = s.errors[self._errNo]
        else: msg = "Undefined Db error"
        if self._extraMsg1 is not None: msg += " (%s)" % self._extraMsg1
        if self._extraMsg2 is not None: msg += " (%s)" % self._extraMsg2
        return msg

    def getErrNo(self):
        return self._errNo

####################################################################################
#### Class Db
####################################################################################
class Db:
    """This class implements the wrapper around MySQLdb. Either host/port or
    socket must be passed. DbName is optional. Password can be empty."""

    def __init__(self, user, passwd=None, host=None, port=None, 
                 socket=None, dbName=None):
        if host is None and port is None and socketIsNone:
            raise DbException(ERR_MISSING_CON_INFO)
        self._conn = None
        self._isConnectedToDb = False
        self._lastFailedConnectAttempt = None
        self._host = host
        self._port = port
        self._user = user
        self._passwd = passwd
        self._socket = socket
        self._dbName = dbName

    def __del__(self):
        self.disconnect()

    ################################################################################
    #### connecting and disconnecting
    ################################################################################

    ### connectToMySQLServer #######################################################
    def connectToMySQLServer(self):
        """Connects to MySQL Server. If socket is available, it will try to use
           it first. If fails, it will then try to use host:port."""
        if self._checkIsConnected(): 
            return
        if self._socket is not None: 
            self._connectThroughSocket()
        else:
            self._connectThroughPort()
        self._lastFailedConnectAttempt = None

    ### _connectThroughSocket ######################################################
    def _connectThroughSocket(self):
        try:
            self._conn = sql.connect(user=self._user,
                                     passwd=self._passwd,
                                     unix_socket=self._socket)
        except MySQLdb.Error as e: # if failed, try port
            if self._host is None and self._port is None:
                msg = "Couldn't connect to MySQL via socket "
                msg += "'%s', " % self._socket
                msg += "and host:port not set, giving up."
                # self._logger.error(msg)
                raise DbException(DbStatus.ERR_MYSQL_CONNECT, msg)
            self._connectThroughPort()
        # self._logger.debug("Connected to MySQL through socket)."

    ### _connectThroughPort ########################################################
    def _connectThroughPort(self):
        try:
            self._conn = sql.connect(user=self._user,
                                     passwd=self._passwd,
                                     host=self._host,
                                     port=self._port)
        except MySQLdb.Error, e2:
            self._conn.close()
            self._conn = None
            msg = "Couldn't connect to MySQL using socket "
            msg += "'%s' or host:port: '%s:%s'. Error: %d: %s." % \
                (self._socket, self._host,self._port, e2.args[0], e2.args[1])
            self._lastFailedConnectAttempt = datetime.now()
            # self._logger.error(msg)
            raise DbException(DbStatus.ERR_MYSQL_CONNECT, msg)
        # self._logger.debug("Connected to MySQL through host/port)."

    ### disconnect #################################################################
    def disconnect(self):
        """It disconnects from the server."""
        if self._conn == None: return
        try:
            self.commit()
            self._conn.close()
        except MySQLdb.Error, e:
            msg = "DB Error %d: %s." % \
                                   (e.args[0], e.args[1])
            # self._logger.error(msg)
            raise DbException(DbStatus.ERR_MYSQL_DISCONN, msg)
        # self._logger.debug("MySQL connection closed.")
        self._conn = None
        self._isConnectedToDb = False

    ################################################################################
    #### dealing with current db: connecting, checking the name
    ################################################################################
    def connectToDb(self, dbName):
        """It connects to a database <dbName>. If it is not connected to the server,
           it will connect first."""
        if self._checkIsConnectedToDb(dbName): return
        try:
            self.connectToMySQLServer()
            self._conn.select_db(dbName)
        except MySQLdb.Error, e:
            # self._logger.debug("Failed to select db '%s'." % self._dbName)
            raise DbException(DbStatus.ERR_CANT_CONNECT_TO_DB, self._dbName)
        self._isConnectedToDb = True
        self._dbName = dbName
        # self._logger.debug("Connected to db '%s'." % self._dbName)

    ################################################################################
    def getDbName(self):
        """It returns current database name."""
        return self._dbName

    ################################################################################
    #### commit
    ################################################################################
    def commit(self):
        """It commits a transaction. Throws exception if it is not connected to the
           server."""
        if not self._checkIsConnected():
            raise DbException(DbStatus.ERR_NOT_CONNECTED)
        self._conn.commit()

    ################################################################################
    #### dealing with databases: create, check if exists, drop
    ################################################################################

    ### checkDbExists ##############################################################
    def checkDbExists(self, dbName):
        """It checks if a given database exists. It will connect to the server if
           not connected already."""
        self.connectToMySQLServer()
        cmd = "SELECT COUNT(*) FROM information_schema.schemata "
        cmd += "WHERE schema_name = '%s'" % dbName
        count = self.execCommand1(cmd)
        return count[0] == 1

    ### createDb ##################################################################
    def createDb(self, dbName):
        """It creates a new database <dbName>. Raises exception if the database
           already exists. It will connect to the server if not connected already.
           Note, it will not connect to that database."""
        self.connectToMySQLServer()
        if self.checkDbExists(dbName):
            raise DbException(DbStatus.ERR_DB_EXISTS)
        self.execCommand0("CREATE DATABASE %s" % dbName)

    ### dropDb #####################################################################
    def dropDb(self, dbName):
        """It drop a database <dbName>. Raises exception if the database
           does not exists. It will connect to the server if not connected
           already. If this is current db, it will disconnect from it."""
        self.connectToMySQLServer()
        if not self.checkDbExists(dbName):
            raise DbException(DbStatus.ERR_DB_DOES_NOT_EXIST)
        self.execCommand0("DROP DATABASE %s" % dbName)
        if self._dbName == dbName:
            self._dbName = None
            self.disconnect()

    ################################################################################
    #### dealing with tables
    ################################################################################

    ### checkTableExists ###########################################################
    def checkTableExists(self, dbName, tableName):
        """It checks if a given table exists in the current database. It will
           connect to the server if not connected already."""
        self.connectToMySQLServer()
        cmd = "SELECT COUNT(*) FROM information_schema.tables "
        cmd += "WHERE table_schema = '%s' AND table_name = '%s'" % \
               (dbName, tableName)
        count = self.execCommand1(cmd)
        return  count[0] == 1

    #### checkTableExists ##########################################################
    def checkTableExists(self, tableName):
        """It checks if a given table exists in the current database. It will
           connect to the server if not connected already."""
        return self.checkTableExists(self._dbName, tableName)

    ### createTable ################################################################
    def createTable(self, tableName, tableSchema):
        """It creates table in current database. It will connect to the server if
           not connected already."""
        self.connectToMySQLServer()
        self.execCommand0("CREATE TABLE %s %s" % (tableName, tableSchema))

    ### createTableInDb ############################################################
    def createTableInDb(self, dbName, tableName, tableSchema):
        """It creates table in the specified database. It will connect to the
           server if not connected already."""
        self.connectToMySQLServer()
        self.execCommand0("CREATE TABLE %s.%s %s" % (dbName,tableName,tableSchema))

    ### printTable #################################################################
    def printTable(self, tableName):
        """It prints the contents of the table. It will connect to the server if
           not connected already."""
        self.connectToMySQLServer()
        ret = self.execCommandN("SELECT * FROM %s" % tableName)
        s = StringIO.StringIO()
        s.write(tableName)
        if len(ret) == 0:
            s.write(" is empty.\n")
        else: 
            s.write(':\n')
        for r in ret:
            print >> s, "   ", r
        return s.getvalue()

    ################################################################################
    #### loadSqlScript
    ################################################################################
    def loadSqlScript(self, scriptPath, dbName):
        """Loads sql script into the database <dbName>."""
        # self._logger.debug("loading script %s into db %s" %(scriptPath,dbName))
        if self._passwd:
            if self._socket is None:
                cmd = 'mysql -h%s -P%s -u%s -p%s %s' % \
                (self._host, self._port, self._user, self._passwd, dbName)
            else:
                cmd = 'mysql -S%s -u%s -p%s %s' % \
                (self._socket, self._user,self._passwd, dbName)
        else:
            if self._socket is None:
                cmd = 'mysql -h%s -P%s -u%s %s' % \
                (self._host, self._port, self._user, dbName)
            else:
                cmd = 'mysql -S%s -u%s %s' % \
                (self._socket, self._user, dbName)
        # self._logger.debug("cmd is %s" % cmd)
        with file(scriptPath) as scriptFile:
            if subprocess.call(cmd.split(), stdin=scriptFile) != 0:
                msg = "Failed to execute %s < %s" % (cmd,scriptPath)
                raise DbException(DbStatus.ERR_CANT_EXEC_SCRIPT, msg)

    ################################################################################
    #### execCommand: no arg, 1 arg, many arg
    ################################################################################
    def execCommand0(self, command):
        """Executes mysql commands which return no rows."""
        self._execCommand(command, 0)

    def execCommand1(self, command):
        """Executes mysql commands which return one row."""
        return self._execCommand(command, 1)

    def execCommandN(self, command):
        """Executes mysql commands which return multiple rows."""
        return self._execCommand(command, 'n')

    ################################################################################
    #### private method: _execCommand
    ################################################################################
    def _execCommand(self, command, nRowsRet):
        """Executes mysql commands which return any number of rows.
        Expected number of returned rows should be given in nRowSet 
        ('0', '1', 'n'). If this function is called after mysqld was
        restarted, or if the connection timed out because of long period 
        of inactivity, the command will fail. This function catches 
        such problems and recovers by reconnecting and retrying."""
        if not self._checkIsConnected():
            self.connectToMySQLServer()

        cursor = self._conn.cursor()
        try:
            # self._logger.debug("Executing '%s'." % command)
            print ("Executing: %s." % command)
            cursor.execute(command)
        except MySQLdb.Error, e:
            self._conn.close()
            self._conn = None
            self._isConnectedToDb = False
            cursor = None
            msg = "MySQL Error [%d]: %s." % (e.args[0], e.args[1])
            raise DbException(DbStatus.ERR_MYSQL_ERROR, msg)
        except MySQLdb.OperationalError as e:
            self._conn.close()
            self._conn = None
            self._isConnectedToDb = False
            cursor = None
            print "'%s' failed. Err: %d: %s. Trying to recover..." % \
                                    (command, e.args[0], e.args[1])

            # make sure we don't try to recover in a tight loop, sleep few sec
            if self._lastFailedConnectAttempt is not None:
                diff = datetime.now() - self._lastFailedConnectAttempt
                diff = diff.seconds + diff.microseconds/1E6
                if diff < 5: sleep(5)
            self._lastFailedConnectAttempt = datetime.now()
            # self._logger.debug("Forcing reconnect.")
            if self._dbName is not None:
                self.connectToDb(self._dbName)
            return self._execCommand(command, nRowsRet)
        if nRowsRet == 0:
            ret = ""
        elif nRowsRet == 1:
            ret = cursor.fetchone()
            # self._logger.debug("Got: %s" % str(ret))
        else:
            ret = cursor.fetchall()
            # self._logger.debug("Got: %s" % str(ret))
        cursor.close()
        return ret

    def _checkIsConnected(self):
        return self._conn != None

    def _checkIsConnectedToDb(self, dbName):
        return (self._checkIsConnected() and
                self._isConnectedToDb and 
                dbName == _self._dbName)

