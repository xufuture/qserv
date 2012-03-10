/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
 
#ifndef LSST_QSERV_WORKER_RESULT_REQUEST_H
#define LSST_QSERV_WORKER_RESULT_REQUEST_H
//#include <deque>
#include <iostream>
#include <string>

//#include <boost/signal.hpp>
#include <boost/shared_ptr.hpp>
//#include <boost/make_shared.hpp>
//#include "lsst/qserv/worker/WorkQueue.h"

namespace lsst {
namespace qserv {
class QservPath; // Forward
namespace worker {

class ResultRequest {
public:
    enum State {UNKNOWN, OPENWAIT, OPEN, OPENERROR};
    typedef boost::shared_ptr<ResultRequest> Ptr;

    explicit ResultRequest(QservPath const& p);
    State getState() const { return _state; }
    std::string getStateStr() const;
private:
    State _accept(QservPath const& p);

    State _state;
    std::string _dumpName;
    std::string _error;

    friend std::ostream& 
        operator<<(std::ostream& os, 
                   lsst::qserv::worker::ResultRequest const& rr); 
};

    
}}} // namespace lsst::qserv::worker

#endif // LSST_QSERV_WORKER_RESULT_REQUEST_H
