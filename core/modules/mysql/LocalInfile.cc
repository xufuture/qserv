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
#include "mysql/LocalInfile.h"

// System headers
#include <cassert>
#include <limits>
#include <stdexcept>
#include <string.h> // for memcpy

// Third-party headers
#include <mysql/mysql.h>

// Qserv headers
#include "mysql/RowBuffer.h"
      
namespace lsst {
namespace qserv {
namespace mysql {
#if 0

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
std::string const mysqlNull("\\N");
// should be less than 0.5 * infileBufferSize
int const largeRowThreshold = 500*1024; 
class RowBuffer {
public:
    RowBuffer(MYSQL_RES* result)
        : _result(result),
          _useLargeRow(false),
          _sep("\t"), _rowSep("\n") {
        // fetch rows.
        _numFields = mysql_num_fields(result);
//        cout << _numFields << " fields per row\n";
    }
    unsigned fetch(char* buffer, unsigned bufLen) {
        unsigned fetchSize = 0;
        unsigned estRowSize = 0;
        if(bufLen <= 0) {
            throw std::invalid_argument("Can't fetch non-positive bytes");
        }
        if(_useLargeRow) {
            return _fetchFromLargeRow(buffer, bufLen);
        }
        // Loop for full rows until buffer is full, or we've detected
        // a large row.
        while(true) {
            // Try to fetch to fill the buffer.
            Row r;
            bool fetchOk = _fetchRow(r);
            if(!fetchOk) {
                return fetchSize;
            }
            estRowSize = _updateEstRowSize(estRowSize, r);
            if(estRowSize > static_cast<unsigned>(largeRowThreshold)) {
                _initializeLargeRow(r);
                unsigned largeFetchSize = _fetchFromLargeRow(buffer + fetchSize, 
                                                         bufLen - fetchSize); 
                return fetchSize + largeFetchSize;
            } else { // Small rows, use simpler row-at-a-time logic
                fetchSize += _addRow(r, 
                                     buffer + fetchSize, 
                                     bufLen - fetchSize);

                fetchSize += _addString(buffer + fetchSize, _rowSep);
                // Ensure 
                if((2 * estRowSize) > (bufLen - fetchSize)) {
                    break;
                }
            }
        }
        return fetchSize;
    }
 
private:
    inline unsigned _updateEstRowSize(unsigned lastRowSize, Row const& r) {
        unsigned rowSize = r.minRowSize();
        if(lastRowSize < rowSize) { 
            return rowSize;
        }
        return lastRowSize;
    }

    unsigned int _addRow(Row r, char* cursor, int remaining) {
        assert(remaining >= 0); // negative remaining is nonsensical
        char* original = cursor;
        unsigned sepSize = _sep.size();
        // 2x orig size to allow escaping + separators +
        // null-terminator for mysql_real_escape_string
        unsigned allocRowSize
            = (2*r.minRowSize()) + ((r.numFields-1) * sepSize) + 1;
        if(allocRowSize > static_cast<unsigned>(remaining)) {
            // Make buffer size in LocalInfile larger than largest
            // row.
            // largeRowThreshold should prevent this.
            throw "Buffer too small for row"; 
        }

        for(int i=0; i < r.numFields; ++i) {
            if(i) {  // add separator
                cursor += _addString(cursor, _sep);
            }
            cursor +=  _addColumn(cursor, r.row[i], r.lengths[i]);
        }
        assert(cursor > original);
        return cursor - original;
    }


    inline int _addString(char* cursor, std::string const& s) {
        int sSize = s.size();
        memcpy(cursor, s.data(), sSize);
        return sSize;
    }

    inline int _escapeString(char* dest, char* src, int srcLength) {
        //mysql_real_escape_string(_mysql, cursor, col, r.lengths[i]);
        assert(srcLength >= 0);
        assert(srcLength < std::numeric_limits<int>::max() / 2);
        char* end = src + srcLength;
        while(src != end) {
            switch(*src) {
            case '\0': *dest++ = '\\'; *dest++ = '0'; break;
            case '\b': *dest++ = '\\'; *dest++ = 'b'; break;
            case '\n': *dest++ = '\\'; *dest++ = 'n'; break;
            case '\r': *dest++ = '\\'; *dest++ = 'r'; break;
            case '\t': *dest++ = '\\'; *dest++ = 't'; break;
            case '\032': *dest++ = '\\'; *dest++ = 'Z'; break; 
            default: *dest++ = *src; break;
                // Null (\N) is not treated by escaping in this context.
            }
        }
        return dest - src;
    }
    inline int _maxColFootprint(int columnLength) {
        const int overhead = 2 + _sep.size(); // NULL decl + sep size
        return overhead + (2 * columnLength);
    }
    inline int _addColumn(char* cursor, char* colData, int colSize) { 
        int added = 0;
        if(colData) {
            // Sanitize field.
            // Don't need mysql_real_escape_string, because we can
            // use the simple LOAD DATA INFILE escaping rules
            added = _escapeString(cursor, colData, colSize);
        } else {
            added = _addString(cursor, mysqlNull);
        }
        return added;
    }        
    bool _fetchRow(Row& r) {
        MYSQL_ROW mysqlRow = mysql_fetch_row(_result);
        if(!mysqlRow) {
            return false;
        }
        r.row = mysqlRow;
        r.lengths = mysql_fetch_lengths(_result);
        r.numFields = _numFields;
        assert(r.lengths);
        return true;
    }

    unsigned _fetchFromLargeRow(char* buffer, int bufLen) {
       // Insert field-at-a-time, 
       char* cursor = buffer;
       int remaining = bufLen;

       while(_maxColFootprint(_largeRow.lengths[_fieldOffset]) > remaining) {
           int addLength = _addColumn(cursor, 
                                      _largeRow.row[_fieldOffset], 
                                      _largeRow.lengths[_fieldOffset]);
           cursor += addLength;
           remaining -= addLength;
           ++_fieldOffset;
           if(_fieldOffset >= _numFields) {
               if(!_fetchRow(_largeRow)) {
                   break; 
                   // no more rows, return what we have
               }
               _fieldOffset = 0;
           }
           if(_maxColFootprint(_largeRow.lengths[_fieldOffset]) > remaining) {
               break;
           }
           // FIXME: unfinished           
       } 
       if(cursor == buffer) { // Were we able to put anything in?           
           throw "Buffer too small for single column!";
       }
       return bufLen - remaining;
   }
    void _initializeLargeRow(Row const& largeRow) {
        _useLargeRow = true;
        _fetchRow(_largeRow);
        _fieldOffset = 0;
    }

    MYSQL_RES* _result;
    bool _useLargeRow;
    int _numFields;

    // Large-row support
    Row _largeRow;
    int _fieldOffset;

    std::string _sep;
    std::string _rowSep;
};
#endif // comment-out Row, RowBuffer

////////////////////////////////////////////////////////////////////////
// LocalInfile implementation
////////////////////////////////////////////////////////////////////////
int const infileBufferSize = 1024*1024; // 1M buffer
LocalInfile::LocalInfile(char const* filename, MYSQL_RES* result)
    : _filename(filename),
      _result(result) {
    // Should have buffer >= sizeof(single row)
    const int defaultBuffer = infileBufferSize;
    _buffer = new char[defaultBuffer];
    _bufferSize = defaultBuffer;
    _leftover = 0;
    _leftoverSize = 0;
    assert(result);
    _rowBuffer = new RowBuffer(result);
}

LocalInfile::~LocalInfile() {
    if(_buffer) {
        delete[] _buffer;
    }
    if(_rowBuffer) {
        delete _rowBuffer;
    }
}

int LocalInfile::read(char* buf, unsigned int bufLen) {
    assert(_rowBuffer);
    // Read into *buf
    unsigned copySize = bufLen;
    unsigned copied = 0;
    if(_leftoverSize) { // Try the leftovers first
        if(copySize > _leftoverSize) {
            copySize = _leftoverSize;
        }
        ::memcpy(buf, _leftover, copySize);
        copied = copySize;
        buf += copySize;
        bufLen -= copySize;
        _leftover += copySize;
        _leftoverSize -= copySize;
    }
    if(bufLen > 0) { // continue?
        // Leftover couldn't satisfy bufLen, so it's empty.
        // Re-fill the buffer.

        unsigned fetchSize = _rowBuffer->fetch(_buffer, _bufferSize);
        if(fetchSize == 0) {
            return copied;
        }
        if(fetchSize > bufLen) { // Fetched more than the buffer
            copySize = bufLen;
        } else {
            copySize = fetchSize;
        }
        ::memcpy(buf, _buffer, copySize);
        copied += copySize;
        _leftover = _buffer + copySize;
        _leftoverSize = fetchSize - copySize;
    }
    return copied;
}

int LocalInfile::getError(char* buf, unsigned int bufLen) {
    return 0;
}
////////////////////////////////////////////////////////////////////////
// LocalInfile::Mgr
////////////////////////////////////////////////////////////////////////
void LocalInfile::Mgr::attach(MYSQL* mysql) {
    mysql_set_local_infile_handler(mysql,
                                   local_infile_init,
                                   local_infile_read,
                                   local_infile_end,
                                   local_infile_error,
                                   this);
}

void LocalInfile::Mgr::detachReset(MYSQL* mysql) {
    mysql_set_local_infile_default(mysql);
}

void LocalInfile::Mgr::prepareSrc(std::string const& filename, MYSQL_RES* result) {
    _map[filename] = result;
}

std::string LocalInfile::Mgr::prepareSrc(MYSQL_RES* result) {
    std::string f = _nextFilename();
    _map[f] = result;
    return f;
}

// mysql_local_infile_handler interface

int LocalInfile::Mgr::local_infile_init(void **ptr, const char *filename, void *userdata) {
    assert(userdata);
    //cout << "New infile:" << filename << "\n";
    LocalInfile::Mgr* m = static_cast<LocalInfile::Mgr*>(userdata);
    MYSQL_RES* r = m->get(std::string(filename));
    assert(r);
    LocalInfile* lf = new LocalInfile(filename, r);
    *ptr = lf;
    if(!lf->isValid()) {
        return 1;
    }
    return 0;
    // userdata points at attached LocalInfileShared
}

int LocalInfile::Mgr::local_infile_read(void *ptr, char *buf, unsigned int buf_len) {
    return static_cast<LocalInfile*>(ptr)->read(buf, buf_len);
}

void LocalInfile::Mgr::local_infile_end(void *ptr) {
    LocalInfile* lf = static_cast<LocalInfile*>(ptr);
    delete lf;
}

int LocalInfile::Mgr::local_infile_error(void *ptr,
                                         char *error_msg,
                                         unsigned int error_msg_len) {
    return static_cast<LocalInfile*>(ptr)->getError(error_msg,
                                                    error_msg_len);
}

}}} // namespace lsst::qserv::mysql

