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
 - user authentication
 - many commands still need to be implemented
 - need to separate dangerous admin commands like DROP EVERYTHING
 - implement proper logging instead of print
"""

import os
import re
import readline
import ConfigParser

from qserv_admin_impl import QservAdminImpl

# This helps if kazoo needs to generate an errror, otherwise we'd get:
# No handlers could be found for logger "kazoo.recipe.watchers"
import logging
logging.basicConfig()


####################################################################################
####################################################################################
####################################################################################
class QAdmException(Exception):
    """
    QAdmException class defines qserv_admin-specific exception
    """
    SUCCESS                     =    0
    ERR_BAD_CMD                 = 3001
    ERR_CONFIG_NOT_FOUND        = 3002
    ERR_MISSING_PARAM           = 3003
    ERR_WRONG_PARAM             = 3004
    ERR_WRONG_PARAM_VALUE       = 3005
    ERR_INTERNAL                = 9999

    ### __init__ ###################################################################
    def __init__(self, errNo, extraMsgList=None):
        """
        Initialize the shared data.

        @param errNo      Error number.
        @param extraMsgList  Optional list of extra messages.
        """
        self._errNo = errNo
        self._extraMsgList = extraMsgList

        self._errors = { 
            ERR_BAD_CMD: ("Bad command, see HELP for details."),
            ERR_CONFIG_NOT_FOUND: ("Config file not found."),
            ERR_MISSING_PARAM: ("Missing parameter."),
            ERR_WRONG_PARAM: ("Unrecognized parameter."),
            ERR_WRONG_PARAM_VALUE: ("Unrecognized value for parameter."),
            ERR_INTERNAL: "Internal error."
        }

    ### __str__ ####################################################################
    def __str__(self):
        """
        Return string representation of the error.

        @return string  Error message string, including all optional messages.
        """
        msg = self._errors.get(self._errNo, "Undefined qserv_admin error")
        if self._extraMsgList is not None:
            for s in self._extraMsgList: msg += " (%s)" % s
        return msg

    ### __str__ ####################################################################
    def __str__(self):
        """
        Return string representation of the error.

        @return string  Error message string, including all optional messages.
        """
        return self._errNo

####################################################################################
####################################################################################
####################################################################################
class CommandParser(object):
    """
    Parse commands and calls appropriate function from qserv_admin_impl.
    """

    ### __init__ ###################################################################
    def __init__(self):
        """
        Initialize shared metadata, including list of supported commands.
        """
        self._funcMap = {
            'CREATE':  self._parseCreate,
            'DROP':    self._parseDrop,
            'HELP':    self._printHelp,
            'RELEASE': self._parseRelease,
            'SHOW':    self._parseShow
            }
        self._impl = QservAdminImpl()
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

    ### receiveCommands ############################################################
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
                except QAdmException as e1:
                    print "ERROR: ", e1
                sql = sql[pos+1:]

    ### main parser PRIVATE ########################################################
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
            print "Unsupported command '%s', see HELP for details." % cmd

    ### _parseCreate PRIVATE #######################################################
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
            print "CREATE '%s' is not supported yet." % t

    ### _parseCreateDatabase #######################################################
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
                raise QAdmException(QAdmStatus.ERR_BAD_CMD, 
                                    ["expected 'LIKE', found: '%s'" % tokens[1]])
            dbName = tokens[0]
            dbName2 = tokens[2]
            self._impl.createDbLike(dbName, dbName2)
        else:
            raise QAdmException(QAdmStatus.ERR_BAD_CMD, 
                                ["unexpected number of arguments"])

    ### _parseCreateTable ##########################################################
    def _parseCreateTable(self, tokens):
        """
        Subparser, handle all CREATE TABLE requests.
        """
        print 'CREATE TABLE not implemented.'

    ### _parseDrop #################################################################
    def _parseDrop(self, tokens):
        """
        Subparser, handle all DROP requests.
        """
        t = tokens[0].upper()
        l = len(tokens)
        if t == 'DATABASE':
            if l != 2:
                raise QAdmException(QAdmStatus.ERR_BAD_CMD,  
                                    ["unexpected number of arguments"])
            self._impl.dropDb(tokens[1])
        elif t == 'TABLE':
            print "drop table not implemented" 
        elif t == 'EVERYTHING':
            self._impl.dropEverything()
        else:
            print "DROP '%s' is not supported yet." % t

    ### _printHelp #################################################################
    def _printHelp(self, tokens):
        """
        Print available commands.
        """
        print self._supportedCommands

    ### _parseRelease ##############################################################
    def _parseRelease(self, tokens):
        """
        Subparser, handle all RELEASE requests.
        """
        print 'RELEASE not implemented.'

    ### _parseShow #################################################################
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
            print "SHOW '%s' is not supported yet." % t

    ### _createDb ##################################################################
    def _createDb(self, dbName, configFile):
        """
        Create database through config file.
        """
        print "Creating db '%s' using config '%s'" % (dbName, configFile)
        options = self._fetchOptionsFromConfigFile(configFile)
        print "options are:", options
        self._impl.createDb(dbName, options)

    ### _fetchOptionsFromConfigFile ################################################
    def _fetchOptionsFromConfigFile(self, fName):
        """
        Read config file <fName> for createDb and createTable command, and return
        key-value pair dictionary (flat, e.g., sections are ignored.)
        """
        if not os.access(fName, os.R_OK):
            raise QAdmException(QAdmStatus.ERR_CONFIG_NOT_FOUND, [fName])
        config = ConfigParser.ConfigParser()
        config.optionxform = str # case sensitive
        config.read(fName)
        xx = {}
        for section in config.sections():
            for option in config.options(section):
                xx[option] = config.get(section, option)
        return xx

    ### _processDbOptions ##########################################################
    def _processDbOptions(self, opts):
        """
        Validate options used by createDb, add default values for missing
        parameters.
        """
        if not opts.has_key("clusteredIndex"):
            print("param 'clusteredIndex' not found, will use default: ''")
            opts["clusteredIndex"] = ''
        if not opts.has_key("partitioning"):
            print("param 'partitioning' not found, will use default: off")
            opts["partitioning"] = "off"
        if not opts.has_key("objectIdIndex"):
            print("param 'objectIdIndex' not found, will use default: ''")
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

    ### _validateKVOptions #########################################################
    def _validateKVOptions(self, x, xxOpts, psOpts, whichInfo):
        if not x.has_key("partitioning"):
            raise QAdmException(QAdmStatus.ERR_MISSING_PARAM, ["partitioning"])

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
                    raise QAdmException(QAdmStatus.ERR_MISSING_PARAM, [o])
        if partOff:
            return
        if x["partitioning"] != "on":
            raise QAdmException(QAdmStatus.ERR_WRONG_PARAM_VALUE,
                                ["partitioning", 
                                "got: '%s'" % x["partitioning"],
                                "expecting: on/off"])

        if not x.has_key("partitioningStrategy"):
            raise QAdmException(QAdmStatus.ERR_MISSING_PARAM,
                                ["partitioningStrategy",
                                "(required if partitioning is on)"])

        psFound = False
        for (psName, theOpts) in psOpts.items():
            if x["partitioningStrategy"] == psName:
                psFound = True
                # check if all required options are specified
                for o in theOpts:
                    if not x.has_key(o):
                        raise QAdmException(QAdmStatus.ERR_MISSING_PARAM, [o])

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
                        raise QAdmException(QAdmStatus.ERR_WRONG_PARAM, [o])
        if not psFound:
            raise QAdmException(QAdmStatus.ERR_WRONG_PARAM,
                                [x["partitioningStrategy"]])

####################################################################################
####################################################################################
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
####################################################################################
####################################################################################
def main():
    try:
        CommandParser().receiveCommands()
    except(KeyboardInterrupt, SystemExit, EOFError):
        print ""

if __name__ == "__main__":
    main()
