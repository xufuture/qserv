#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013 LSST Corporation.
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
qserv client program used by all users that talk to qserv. A thin shell that parses
commands, reads all input data in the form of config files into arrays, and calls
corresponding function.

@author  Jacek Becla, SLAC


Known issues and todos:
 - deal with user authentication
 - many commands still need to be implemented
 - need to separate dangerous admin commands like DROP EVERYTHING
"""

import logging
import os
from optparse import OptionParser
import re
import readline
import ConfigParser

from qserv_admin_impl import QservAdminImpl

class QAdmException(Exception):
    """
    Defines qserv_admin-specific exception.
    """
    SUCCESS                     =    0
    ERR_BAD_CMD                 = 3001
    ERR_CONFIG_NOT_FOUND        = 3002
    ERR_MISSING_PARAM           = 3003
    ERR_WRONG_PARAM             = 3004
    ERR_WRONG_PARAM_VAL         = 3005
    ERR_INTERNAL                = 9999

    def __init__(self, errNo, extraMsgList=None):
        """
        Initialize the shared data.

        @param errNo      Error number.
        @param extraMsgList  Optional list of extra messages.
        """
        self._errNo = errNo
        self._extraMsgList = extraMsgList

        self._errors = { 
            QAdmException.ERR_BAD_CMD: ("Bad command, see HELP for details."),
            QAdmException.ERR_CONFIG_NOT_FOUND: ("Config file not found."),
            QAdmException.ERR_MISSING_PARAM: ("Missing parameter."),
            QAdmException.ERR_WRONG_PARAM: ("Unrecognized parameter."),
            QAdmException.ERR_WRONG_PARAM_VAL: "Unrecognized value for parameter.",
            QAdmException.ERR_INTERNAL: "Internal error."
        }

    def __str__(self):
        """
        Return string representation of the error.

        @return string  Error message string, including all optional messages.
        """
        msg = self._errors.get(self._errNo, "Undefined qserv_admin error")
        if self._extraMsgList is not None:
            for s in self._extraMsgList: msg += " (%s)" % s
        return msg

####################################################################################
class CommandParser(object):
    """
    Parse commands and calls appropriate function from qserv_admin_impl.
    """

    def __init__(self, verbosityT):
        """
        Initialize shared metadata, including list of supported commands.

        @param     verbosityT   Verbosity threshold. Logging messages which are
        less severe than verbosityT will be ignored. Expected values match python
        logging numeric values (CRITICAL=50, ERROR=40, WARNING=30, INFO=20,
        DEBUG=10, NOTSET=0).
        """
        self._initLogging(verbosityT)
        self._funcMap = {
            'CREATE':  self._parseCreate,
            'DROP':    self._parseDrop,
            'HELP':    self._printHelp,
            'RELEASE': self._parseRelease,
            'SHOW':    self._parseShow
            }
        self._impl = QservAdminImpl(self._loggerName, verbosityT)
        self._supportedCommands = """
  Supported commands:
    CREATE DATABASE <dbName> <configFile>;
    CREATE DATABASE <dbName> LIKE <dbName2>;
    DROP DATABASE <dbName>;
    DROP EVERYTHING;
    SHOW DATABASES;
    SHOW EVERYTHING;
    QUIT;
    EXIT;
    ...more coming soon
"""

    def receiveCommands(self):
        """
        Receive user commands. End of command is determined by ';'. Multiple
        commands per line are allowed. Multi-line commands are allowed. To
        terminate: CTRL-D, or "exit;" or quit;".
        """
        line = ''
        sql = ''
        while True:
            line = raw_input("qserv > ")
            sql += line.strip()+' '
            while re.search(';', sql):
                pos = sql.index(';')
                try:
                    self._parse(sql[:pos])
                except QAdmException as e:
                    self._logger.error(e)
                sql = sql[pos+1:]

    def _parse(self, cmd):
        """
        Parser, dispatch to subparsers based on first word. Raise exceptions on
        errors.
        """
        cmd = cmd.strip()
        # ignore empty commands, these can be generated by typing ;;
        if len(cmd) == 0: return 
        tokens = cmd.split()
        t = tokens[0].upper()
        if t in self._funcMap:
            self._funcMap[t](tokens[1:])
        elif t == 'EXIT' or t == 'QUIT':
            raise SystemExit()
        else:
            self._logger.error("Unsupported command '%s', see HELP." % cmd)

    def _parseCreate(self, tokens):
        """
        Subparser, handle all CREATE requests.
        """
        t = tokens[0].upper()
        if t == 'DATABASE':
            self._parseCreateDatabase(tokens[1:])
        elif t == 'TABLE':
            self._parseCreateTable(tokens[1:])
        else:
            self._logger.error("CREATE '%s' is not supported yet." % t)

    def _parseCreateDatabase(self, tokens):
        """
        Subparser, handle all CREATE DATABASE requests.
        """
        l = len(tokens)
        if l == 2:
            dbName = tokens[0]
            configFile = tokens[1]
            options = self._fetchOptionsFromConfigFile(configFile)
            options = self._processDbOptions(options)
            self._impl.createDb(dbName, options)
        elif l == 3:
            if tokens[1].upper() != 'LIKE':
                raise QAdmException(QAdmException.ERR_BAD_CMD, 
                                    ["expected 'LIKE', found: '%s'." % tokens[1]])
            dbName = tokens[0]
            dbName2 = tokens[2]
            self._impl.createDbLike(dbName, dbName2)
        else:
            raise QAdmException(QAdmException.ERR_BAD_CMD, 
                                ["unexpected number of arguments."])

    def _parseCreateTable(self, tokens):
        """
        Subparser, handle all CREATE TABLE requests.
        """
        self._logger.error('CREATE TABLE not implemented.')

    def _parseDrop(self, tokens):
        """
        Subparser, handle all DROP requests.
        """
        t = tokens[0].upper()
        l = len(tokens)
        if t == 'DATABASE':
            if l != 2:
                raise QAdmException(QAdmException.ERR_BAD_CMD,  
                                    ["unexpected number of arguments"])
            self._impl.dropDb(tokens[1])
        elif t == 'TABLE':
            self._logger.error("drop table not implemented")
        elif t == 'EVERYTHING':
            self._impl.dropEverything()
        else:
            self._logger.error("DROP '%s' is not supported yet." % t)

    def _printHelp(self, tokens):
        """
        Print available commands.
        """
        print self._supportedCommands

    def _parseRelease(self, tokens):
        """
        Subparser, handle all RELEASE requests.
        """
        self._logger.error('RELEASE not implemented.')

    def _parseShow(self, tokens):
        """
        Subparser, handle all SHOW requests.
        """
        t = tokens[0].upper()
        if t == 'DATABASES':
            self._impl.showDatabases()
        elif t == 'EVERYTHING':
            self._impl.showEverything()
        else:
            self._logger.error("SHOW '%s' is not supported yet." % t)

    def _createDb(self, dbName, configFile):
        """
        Create database through config file.
        """
        self._logger.info("Creating db '%s' using config '%s'" % \
                              (dbName, configFile))
        options = self._fetchOptionsFromConfigFile(configFile)
        self._impl.createDb(dbName, options)

    def _fetchOptionsFromConfigFile(self, fName):
        """
        Read config file <fName> for createDb and createTable command, and return
        key-value pair dictionary (flat, e.g., sections are ignored.)
        """
        if not os.access(fName, os.R_OK):
            raise QAdmException(QAdmException.ERR_CONFIG_NOT_FOUND, [fName])
        config = ConfigParser.ConfigParser()
        config.optionxform = str # case sensitive
        config.read(fName)
        xx = {}
        for section in config.sections():
            for option in config.options(section):
                xx[option] = config.get(section, option)
        return xx

    def _processDbOptions(self, opts):
        """
        Validate options used by createDb, add default values for missing
        parameters.
        """
        if not opts.has_key("clusteredIndex"):
            self._logger.info(
                "param 'clusteredIndex' not found, will use default: ''")
            opts["clusteredIndex"] = ''
        if not opts.has_key("partitioning"):
            self._logger.info(
                "param 'partitioning' not found, will use default: off")
            opts["partitioning"] = "off"
        if not opts.has_key("objectIdIndex"):
            self._logger.info(
                "param 'objectIdIndex' not found, will use default: ''")
            opts["objectIdIndex"] = ''
        # these are required options for createDb
        _crDbOpts = { 
            "db_info": ("level", 
                        "partitioning", 
                        "partitioningStrategy")}
        _crDbPSOpts = {
            "sphBox":("nStripes", 
                      "nSubStripes", 
                      "overlap")}
        # validate the options
        self._validateKVOptions(opts, _crDbOpts, _crDbPSOpts, "db_info")
        return opts

    def _validateKVOptions(self, x, xxOpts, psOpts, whichInfo):
        if not x.has_key("partitioning"):
            raise QAdmException(QAdmException.ERR_MISSING_PARAM, ["partitioning"])

        partOff = x["partitioning"] == "off" 
        for (theName, theOpts) in xxOpts.items():
            for o in theOpts:
                # skip optional parameters
                if o == "partitioning":
                    continue
                # if partitioning is "off", partitioningStrategy does not 
                # need to be specified 
                if not (o == "partitiongStrategy" and partOff):
                    continue
                if not x.has_key(o):
                    raise QAdmException(QAdmException.ERR_MISSING_PARAM, [o])
        if partOff:
            return
        if x["partitioning"] != "on":
            raise QAdmException(QAdmException.ERR_WRONG_PARAM_VAL,
                                ["partitioning", 
                                "got: '%s'" % x["partitioning"],
                                "expecting: on/off"])

        if not x.has_key("partitioningStrategy"):
            raise QAdmException(QAdmException.ERR_MISSING_PARAM,
                                ["partitioningStrategy",
                                "(required if partitioning is on)"])

        psFound = False
        for (psName, theOpts) in psOpts.items():
            if x["partitioningStrategy"] == psName:
                psFound = True
                # check if all required options are specified
                for o in theOpts:
                    if not x.has_key(o):
                        raise QAdmException(QAdmException.ERR_MISSING_PARAM, [o])

                # check if there are any unrecognized options
                for o in x:
                    if not ((o in xxOpts[whichInfo]) or (o in theOpts)):
                        # skip non required, these are not in xxOpts/theOpts
                        if whichInfo=="db_info" and o=="clusteredIndex":
                            continue
                        if whichInfo=="db_info" and o=="objectIdIndex":
                            continue
                        if whichInfo=="table_info" and o=="partitioningStrategy":
                            continue
                        raise QAdmException(QAdmException.ERR_WRONG_PARAM, [o])
        if not psFound:
            raise QAdmException(QAdmException.ERR_WRONG_PARAM,
                                [x["partitioningStrategy"]])

    def _initLogging(self, verbosityT):
        self._loggerName = "QADM"
        self._logger = logging.getLogger(self._loggerName)
        kL = os.getenv('KAZOO_LOGGING')
        if kL: logging.getLogger("kazoo.client").setLevel(int(kL))

####################################################################################
class VolcabCompleter:
    """
    Set auto-completion for commonly used words.
    """
    def __init__(self, volcab):
        self.volcab = volcab

    def complete(self, text, state):
        results = [x+' ' for x in self.volcab 
                   if x.startswith(text.upper())] + [None]
        return results[state]

readline.parse_and_bind("tab: complete")
words = ['CONFIG',
         'CREATE',
         'DATABASE',
         'DATABASES',
         'DROP',
         'INTO',
         'LIKE',
         'LOAD',
         'RELEASE',
         'SHOW',
         'TABLE']
completer = VolcabCompleter(words)
readline.set_completer(completer.complete)

####################################################################################
class SimpleOptionParser:
    """
    Parse command line options.
    """

    def __init__(self):
        self._verbosityT = 40 # default is ERROR
        self._logFileName = None
        self._usage = \
"""

NAME
        qserv_admin - the client program for Central State System (CSS)

SYNOPSIS
        qserv_admin [OPTIONS]

OPTIONS
   -v
        Verbosity threshold. Logging messages which are less severe than
        provided will be ignored. Expected value range: 0=50: (CRITICAL=50,
        ERROR=40, WARNING=30, INFO=20, DEBUG=10). Default value is ERROR.
   -f
        Name of the output log file. If not specified, the output goes to stderr.
"""

    def getVerbosityT(self):
        """
        Return verbosity threshold.
        """
        return self._verbosityT

    def getLogFileName(self):
        """
        Return the name of the output log file.
        """
        return self._logFileName

    def parse(self):
        """
        Parse options.
        """
        parser = OptionParser(usage=self._usage)
        parser.add_option("-v", dest="verbT")
        parser.add_option("-f", dest="logF")
        (options, args) = parser.parse_args()
        if options.verbT: 
            self._verbosityT = int(options.verbT)
            if   self._verbosityT > 50: self._verbosityT = 50
            elif self._verbosityT <  0: self._verbosityT = 0
        if options.logF:
            self._logFileName = options.logF

####################################################################################
def main():
    # parse arguments
    p = SimpleOptionParser()
    p.parse()

    # configure logging
    if p.getLogFileName():
        logging.basicConfig(
            filename=p.getLogFileName(),
            format='%(asctime)s %(name)s %(levelname)s: %(message)s', 
            datefmt='%m/%d/%Y %I:%M:%S', 
            level=p.getVerbosityT())
    else:
        logging.basicConfig(
            format='%(asctime)s %(name)s %(levelname)s: %(message)s', 
            datefmt='%m/%d/%Y %I:%M:%S', 
            level=p.getVerbosityT())

    # wait for commands and process
    try:
        CommandParser(p.getVerbosityT()).receiveCommands()
    except(KeyboardInterrupt, SystemExit, EOFError):
        print ""

if __name__ == "__main__":
    main()
