#include "Csv.h"

#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <stdexcept>


namespace dupr {

char * parseLine(char * beg, char * end, char delim, char **fields, int n) {
    if (end <= beg) {
        throw std::logic_error("line ends before it begins!?");
    }
    if (n < 1) {
        throw std::logic_error("field count < 1!?");
    }
    bool saw_quote = false;
    bool saw_escape = false;
    fields[0] = beg;
    int i = 1;
    for (; beg < end; ++beg) {
        if (*beg == '\n') {
            ++beg;
            break;
        }
        if (saw_escape) {
            saw_escape = false;
        } else if (saw_quote) {
            saw_escape = (*beg == '\\');
            saw_quote = (*beg != '"');
        } else {
            saw_escape = (*beg == '\\');
            saw_quote = (*beg == '"');
            if (*beg == delim) {
                if (i >= n) {
                    throw std::runtime_error("too many fields in line");
                }
                fields[i] = beg + 1;
                ++i;
            }
        }
    }
    if (saw_quote || saw_escape) {
        throw std::runtime_error("invalid line format: embedded new-line, "
                                 "trailing escape, or missing quote");
    }
    if (i != n) {
        throw std::runtime_error("line does not contain expected number of fields");
    }
    fields[i] = beg;
    return beg;
}


bool isNull(char * beg, char * end) {
    while (beg < end && isspace(*beg)) { ++beg; }
    while (end - 1 > beg && isspace(*(end - 1))) { --end; }
    if (end <= beg) {
        return true;
    }
    size_t n = static_cast<size_t>(end - beg);
    if (n == 2 && beg[0] == '\\' && beg[1] == 'N') {
        return true;
    } else if (n == 4 && beg[0] == 'N' && beg[1] == 'U' && beg[2] == 'L' && beg[3] == 'L') {
        return true;
    }
    return false;
}


double extractDouble(char * beg, char * end) {
    char buf[64];
    while (beg < end && isspace(*beg)) { ++beg; }
    while (end - 1 > beg && isspace(*(end - 1))) { --end; }
    if (end <= beg) {
        throw std::runtime_error("cannot convert empty field to a double");
    }
    size_t n = static_cast<size_t>(end - beg);
    if (n + 1 > sizeof(buf)) {
        throw std::runtime_error("field contains too many characters");
    }
    memcpy(buf, beg, n);
    buf[n] = '\0';
    errno = 0;
    char *endptr = 0;
    double val = strtod(buf, &endptr);
    if (errno != 0 || endptr == beg || endptr != buf + n) {
        throw std::runtime_error("failed to convert field to a double");
    }
    return val;
}


int64_t extractInt(char * beg, char * end) {
    char buf[32];
    while (beg < end && isspace(*beg)) { ++beg; }
    while (end - 1 > beg && isspace(*(end - 1))) { --end; }
    if (end <= beg) {
        throw std::runtime_error("cannot convert empty field to an integer");
    }
    size_t n = static_cast<size_t>(end - beg);
    if (n + 1 > sizeof(buf)) {
        throw std::runtime_error("field contains too many characters");
    }
    memcpy(buf, beg, n);
    buf[n] = '\0';
    errno = 0;
    char *endptr = 0;
    long long val = strtoll(buf, &endptr, 10);
    if (errno != 0 || endptr == beg || endptr != buf + n) {
        throw std::runtime_error("failed to convert field to an integer");
    }
    return static_cast<int64_t>(val);
}

} // namespace dupr
