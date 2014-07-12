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
#include <sstream>

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
class StringChannel : public SendChannel {
public:
    StringChannel(std::string& dest) : _dest(dest) {}

    virtual void send(char const* buf, int bufLen) {
        _dest.append(buf, bufLen);
    }

    virtual void sendError(std::string const& msg, int code) {
        std::ostringstream os;
        os << "(" << code << "," << msg << ")";
        _dest.append(os.str());
    }
    virtual void sendFile(int fd, Size fSize) {
        Size bytesRead = 0;
        char buf[fSize];
        Size remain = fSize;
        while(remain > 0) {
            Size frag = ::read(fd, buf, remain);
            if(frag < 0) {
                std::cout << "ERROR reading from fd during "
                          << "StringChannel::sendFile(" << "," << fSize << ")";
                break;
            } else if(frag == 0) {
                std::cout << "ERROR unexpected 0==read() during "
                          << "StringChannel::sendFile(" << "," << fSize << ")";
                break;
            }
            _dest.append(buf, frag);
            remain -= frag;
        }
    }
    virtual void sendStream(char const* buf, int bufLen, bool last) {
        _dest.append(buf, bufLen);
        std::cout << "StringChannel sendStream(" << (void*) buf
                  << ", " << bufLen << ", "
                  << (last ? "true" : "false") << ");\n";
    }
private:
    std::string& _dest;
};

boost::shared_ptr<SendChannel> SendChannel::newStringChannel(std::string& d) {
    return boost::shared_ptr<StringChannel>(new StringChannel(d));
}

}}} // namespace lsst::qserv::wbase
