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
This module defines errors codes with descriptions and the Exception class
for Central State Service (CSS).
"""


class Status:
    SUCCESS                     = 0
    ERR_KEY_ALREADY_EXISTS      = 2001
    ERR_KEY_INVALID             = 2002
    ERR_KEY_DOES_NOT_EXIST      = 2003
    ERR_NOT_IMPLEMENTED         = 9998
    ERR_INTERNAL                = 9999

    errors = { 
        ERR_NOT_IMPLEMENTED: ("This feature is not implemented yet."),
        ERR_INTERNAL: "Internal error."
        }

def getErrMsg(errNo):
    s = Status()
    if errNo in s.errors:
        return s.errors[errNo]
    return "Undefined css error"

class CssException(Exception):
    def __init__(self, errNo, extraMsg1=None, extraMsg2=None):
        self._errNo = errNo
        self._extraMsg1 = extraMsg1
        self._extraMsg2 = extraMsg2

    def getErrMsg(self):
        msg = getErrMsg(self._errNo)
        if self._extraMsg1 is not None:
            msg += " (%s)" % self._extraMsg1
        if self._extraMsg2 is not None:
            msg += " (%s)" % self._extraMsg2
        return msg

    def getErrNo(self):
        return self._errNo
