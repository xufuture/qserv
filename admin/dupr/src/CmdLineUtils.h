/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

/// \file
/// \brief Command-line utility functions.

#ifndef LSST_QSERV_ADMIN_DUPR_CMDLINEUTILS_H
#define LSST_QSERV_ADMIN_DUPR_CMDLINEUTILS_H 

#include "boost/program_options.hpp"


namespace lsst { namespace qserv { namespace admin { namespace dupr {

/// Parse the given command line according to the `options` given and store
/// the results in `vm`. This function defines generic options `help`, `verbose`,
/// `profile`, `config-file`. It handles `help` output and configuration file
/// parsing for the caller.
void parseCommandLine(boost::program_options::variables_map & vm,
                      boost::program_options::options_description const & opts,
                      int argc,
                      char const * const * argv,
                      char const * help);

}}}} // namespace lsst::qserv::admin::dupr

#endif // LSST_QSERV_ADMIN_DUPR_CMDLINEUTILS_H

