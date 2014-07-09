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
#ifndef LSST_QSERV_MYSQL_ROWBUFFER_H
#define LSST_QSERV_MYSQL_ROWBUFFER_H

// System headers
#include <string>

#include <mysql/mysql.h>

namespace lsst {
namespace qserv {
namespace mysql {
////////////////////////////////////////////////////////////////////////
// Row: a mysql row abstraction
////////////////////////////////////////////////////////////////////////
struct Row {
    Row() {}
    // Shallow copies all-around.
    Row(char** row_, unsigned long int* lengths_, int numFields_)
        : row(row_), lengths(lengths_), numFields(numFields_) {}

    unsigned int minRowSize() const {
        unsigned int sum = 0;
        for(int i=0; i < numFields; ++i) {
            sum += lengths[i];
        }
        return sum;
    }

    char** row;
    unsigned long int* lengths;
    int numFields;
};

////////////////////////////////////////////////////////////////////////
// RowBuffer: an buffer from which arbitrarily-sized buckets of bytes
// can be read. The buffer represents a tab-separated-field,
// line-delimited-tuple sequence of tuples streamed out of a MYSQL_RES*
////////////////////////////////////////////////////////////////////////
class RowBuffer {
public:
    RowBuffer(MYSQL_RES* result);
    unsigned fetch(char* buffer, unsigned bufLen);
    unsigned int _addRow(Row r, char* cursor, int remaining);
    bool _fetchRow(Row& r);
    unsigned _fetchFromLargeRow(char* buffer, int bufLen);
    void _initializeLargeRow(Row const& largeRow);
private:
    MYSQL_RES* _result;
    bool _useLargeRow;
    int _numFields;

    // Large-row support
    Row _largeRow;
    int _fieldOffset;

    std::string _sep;
    std::string _rowSep;
};
}}} // lsst::qserv::mysql
#endif // LSST_QSERV_MYSQL_ROWBUFFER_H
