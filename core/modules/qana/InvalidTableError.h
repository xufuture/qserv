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

#ifndef LSST_QSERV_QANA_INVALIDTABLEERROR_H
#define LSST_QSERV_QANA_INVALIDTABLEERROR_H

// System headers
#include <stdexcept>

namespace lsst {
namespace qserv {
namespace qana {

/// `InvalidTableError` is thrown for queries referencing mis-configured or
/// non-existent tables.
class InvalidTableError : public std::runtime_error {
public:
    explicit InvalidTableError(std::string const & what) :
        std::runtime_error(what) {}
};

}}} // namespace lsst::qserv::qana

#endif // LSST_QSERV_QANA_INVALIDTABLEERROR_H
