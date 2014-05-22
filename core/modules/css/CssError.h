// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

/**
  * @file
  *
  * @brief RunTimeErrors for KvInterface.
  *
  * @Author Jacek Becla, SLAC
  */

#ifndef LSST_QSERV_CSS_RUNTIMEERROR_H
#define LSST_QSERV_CSS_RUNTIMEERROR_H

// System headers
#include <map>
#include <stdexcept>
#include <string>

namespace lsst {
namespace qserv {
namespace css {

/**
 * Base class for all CSS run-time errors
 */
class CssRunTimeError : public std::runtime_error {
protected:
    explicit CssRunTimeError(std::string const& msg)
        : std::runtime_error(msg) {}
};

/**
 * Specialized run-time error: database does not exist.
 */
class NoSuchDb : public CssRunTimeError {
public:
    NoSuchDb(std::string const& dbName)
        : CssRunTimeError("Database '" + dbName + "' does not exist.") {}
};

/**
 * Specialized run-time error: key does not exist.
 */
class NoSuchKey : public CssRunTimeError {
public:
    NoSuchKey(std::string const& keyName)
        : CssRunTimeError("Key '" + keyName + "' does not exist.") {}
};

/**
 * Specialized run-time error: table does not exist.
 */
class NoSuchTable : public CssRunTimeError {
public:
    NoSuchTable(std::string const& tableName)
        : CssRunTimeError("Table '" + tableName + "' does not exist.") {}
};

/**
 * Specialized run-time error: authorization failure.
 */
class AuthError : public CssRunTimeError {
public:
    AuthError()
        : CssRunTimeError("Authorization failure.") {}
};

/**
 * Specialized run-time error: connection failure.
 */
class ConnError : public CssRunTimeError {
public:
    ConnError()
        : CssRunTimeError("Failed to connect to persistent store.") {}
};

/**
 * Generic CSS run-time error.
 */
class CssError : public CssRunTimeError {
public:
    CssError(std::string const& extraMsg)
        : CssRunTimeError("Internal run-time error. (" + extraMsg + ")") {}
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_RUNTIMEERROR_H
