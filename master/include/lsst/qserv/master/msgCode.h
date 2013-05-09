// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
 * 
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the LSST License Statement and 
 * the GNU General Public License along with this program.  If not, 
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

#ifndef LSST_QSERV_MASTER_MSGCODE_H
#define LSST_QSERV_MASTER_MSGCODE_H

// Codes for C++ layer are >= 1000.
// (<1000 reserved for Python layer.)
#define MSG_MGR_ADD        1200
#define MSG_XRD_OPEN_FAIL  1290
#define MSG_XRD_WRITE      1300
#define MSG_XRD_READ       1400
#define MSG_MERGED         1500
#define MSG_ERASED         1600
#define MSG_EXEC_SQUASHED  1990
#define MSG_FINALIZED      2000

#endif // LSST_QSERV_MASTER_MSGCODE_H

