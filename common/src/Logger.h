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

// Logger.h: 
// class Logger -- A class that handles application-wide logging.
//
 
#ifndef LSST_QSERV_LOGGER_H
#define LSST_QSERV_LOGGER_H

// These directives are for convenience.
#define LOGGER lsst::qserv::Logger::Instance()
#define LOGGER_DBG lsst::qserv::Logger::Instance(lsst::qserv::Logger::Debug)
#define LOGGER_INF lsst::qserv::Logger::Instance(lsst::qserv::Logger::Info)
#define LOGGER_WRN lsst::qserv::Logger::Instance(lsst::qserv::Logger::Warning)
#define LOGGER_ERR lsst::qserv::Logger::Instance(lsst::qserv::Logger::Error)
#define LOGGER_THRESHOLD_DBG lsst::qserv::Logger::Instance()\
                             .setSeverityThreshold(lsst::qserv::Logger::Debug);
#define LOGGER_THRESHOLD_INF lsst::qserv::Logger::Instance()\
                             .setSeverityThreshold(lsst::qserv::Logger::Info);
#define LOGGER_THRESHOLD_WRN lsst::qserv::Logger::Instance()\
                             .setSeverityThreshold(lsst::qserv::Logger::Warning);
#define LOGGER_THRESHOLD_ERR lsst::qserv::Logger::Instance()\
                             .setSeverityThreshold(lsst::qserv::Logger::Error);

#include <stdio.h>
#include <sys/time.h>
#include <iostream>
#include <boost/iostreams/concepts.hpp>
#include <boost/iostreams/operations.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/line.hpp>
#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>

namespace lsst {
namespace qserv {

class Logger : public boost::iostreams::filtering_ostream {
public:
    enum Severity { Debug = 0, Info, Warning, Error };
    static std::ostream* volatile logStreamPtr;
    static Logger& Instance();
    static Logger& Instance(Severity severity);
    void setSeverity(Severity severity);
    Severity getSeverity() const;
    void setSeverityThreshold(Severity severity);
    Severity getSeverityThreshold() const;

private:
    // Filter class responsible for enforcing severity level.
    class SeverityFilter : public boost::iostreams::multichar_output_filter {
    public:
        SeverityFilter(Logger* loggerPtr);
        template<typename Sink>
        std::streamsize write(Sink& dest, const char* s, std::streamsize n);
    private:
        Logger* _loggerPtr;
    };
            
    // Filter class responsible for formatting Logger output.
    class LogFilter : public boost::iostreams::line_filter {
    public:
        LogFilter(Logger* loggerPtr);
    private:
        std::string do_filter(const std::string& line);
        std::string getThreadId();
        std::string getTimeStamp();
        std::string getSeverity();
        Logger* _loggerPtr;
    };

    // Private constructor, etc. as per singleton pattern.
    Logger();
    Logger(Logger const&);
    Logger& operator=(Logger const&);

    Severity _severity;
    static Severity _severityThreshold; // Application-wide severity threshold.
    static boost::mutex _mutex;

    // Thread local storage to ensure one instance of Logger per thread.
    static boost::thread_specific_ptr<Logger> _instancePtr;

};

}} // lsst::qserv

#endif // LSST_QSERV_LOGGER_H
