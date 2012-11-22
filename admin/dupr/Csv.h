/// Simplified CSV parsing.
#ifndef CSV_H
#define CSV_H

#include <stdint.h>


namespace dupr {

/// Maximum CSV line size.
int const MAX_LINE_SIZE = 16384;

/** Parse a single CSV line, storing offsets to the individual fields.
    The following assumptions must hold:

    - A record is contained in exactly one line, i.e. fields never contain
      embedded new-lines.
    - The line terminator is LF ('\n').
    - The escape character is '\\'.
    - The quote character is '"'.
    - No line is longer than MAX_LINE_SIZE bytes.
    - The character set is not multi-byte. In other words, ASCII or UTF-8
      are supported, but UTF-16 is not.
    - Numeric fields are not quoted.

    After this call, fields will contain n + 1 pointers, where n is the
    number of fields in the line. The i-th field will range from
    [fields[i], fields[i+1] - 1). Note that *(field[i+1] - 1) will be
    a field delimiter or line separator character and must be ignored.

    A pointer to the first character in the next line (or end) is returned.
  */
char * parseLine(char * beg, char * end, char delim, char **fields, int n);

/// Return true if the field [beg, end) corresponds to a NULL.
bool isNull(char * beg, char * end);

/// Return field [beg, end) as a double. Doesn't handle quoted numeric fields.
double extractDouble(char * beg, char * end);

/// Return field [beg, end) as an int64_t. Doesn't handle quoted numeric fields.
int64_t extractInt(char * beg, char * end);

} // namespace dupr

#endif // CSV_H
