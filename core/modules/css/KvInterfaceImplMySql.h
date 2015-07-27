// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
  * @brief Interface to the Common State System - in MySql storage
  *
  * @Author Nathan Pease, SLAC
  */


#ifndef LSST_QSERV_CSS_KVINTERFACEIMPLMYSQL_H
#define LSST_QSERV_CSS_KVINTERFACEIMPLMYSQL_H

// System headers
#include <memory>
#include <string>
#include <vector>

// Local headers
#include "css/KvInterface.h"
#include "mysql/MySqlConfig.h"
#include "sql/SqlConnection.h"

namespace lsst {
namespace qserv {
namespace css {

class KvInterfaceImplMySql : public KvInterface {
public:

    /**
     *  @param mysqlConf: Configuration object for mysql connection
     */
	KvInterfaceImplMySql(mysql::MySqlConfig const& mysqlConf);
	virtual ~KvInterfaceImplMySql() {};

    /**
     * Create a key/value pair.
     * Throws KeyExistsError if the key already exists (or if any other problem,
     * e.g., a connection error is detected).
     */
    virtual void create(std::string const& key, std::string const& value);

    /**
     * Set a key/value pair. If the key already exists, its value is
     * overwritten.
     * Throws CssError when unable to set the pair (error with the underlying
     * persistence).
     */
    virtual void set(std::string const& key, std::string const& value);

    /**
     * Check if the key exists.
     */
    virtual bool exists(std::string const& key);

    /**
     * Returns children (vector of strings) for a given key.
     * Throws CssRunTimeError if the key does not exist (or if any other problem,
     * e.g., a connection error is detected)
     */
    virtual std::vector<std::string> getChildren(std::string const& key);

    /**
     * Delete a key.
     * Throws CssRunTimeError on failure.
     */
    virtual void deleteKey(std::string const& key);

protected:
    virtual std::string _get(std::string const& key,
                             std::string const& defaultValue,
                             bool throwIfKeyNotFound);
private:
    /**
     * Get the value from the server for a given key.
     * Takes an SqlErrorObject that will be populated if the value can not be found.
     * Returns true if key was found, false if key was not found on server.
     */
    bool _getValFromServer(std::string const& key, std::string & val, sql::SqlErrorObject & errObj);

    sql::SqlConnection _conn;
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_KVINTERFACEIMPLMYSQL_H
