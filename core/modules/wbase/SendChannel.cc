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
#include "wbase/SendChannel.h"

// System headers
#include <iostream>

namespace lsst {
namespace qserv {
namespace wbase {

class NopChannel : public SendChannel {
public:
    virtual void send(char const* buf, int bufLen) {
        std::cout << "NopChannel send(" << (void*) buf
                  << ", " << bufLen << ");\n";
    }

    virtual void sendError(std::string const& msg, int code) {
        std::cout << "NopChannel sendError(\"" << msg
                  << "\", " << code << ");\n";
    }
    virtual void sendFile(int fd, Size fSize) {
        std::cout << "NopChannel sendFile(" << fd
                  << ", " << fSize << ");\n";
    }
    virtual void sendStream(char const* buf, int bufLen, bool last) {
        std::cout << "NopChannel sendStream(" << (void*) buf
                  << ", " << bufLen << ", "
                  << (last ? "true" : "false") << ");\n";
    }
};

boost::shared_ptr<SendChannel> SendChannel::newNopChannel() {
    return boost::shared_ptr<NopChannel>(new NopChannel);
}
}}} // namespace lsst::qserv::wbase
