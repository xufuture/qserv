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

#include "CmdLineUtils.h"

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "FileUtils.h"

using std::bad_alloc;
using std::cerr;
using std::cout;
using std::endl;
using std::exit;
using std::free;
using std::logic_error;
using std::malloc;
using std::pair;
using std::runtime_error;
using std::set;
using std::string;
using std::vector;

using boost::shared_ptr;

namespace fs = boost::filesystem;
namespace po = boost::program_options;


namespace {

    // A configuration file parser that understands a very forgiving format
    // resembling JSON. This is necessary because the INI file parser that
    // ships with boost has no escaping mechanism and automatically strips
    // whitespace from option values. This makes it impossible, as an example,
    // to set the value of a field separator character option to the TAB
    // character in a configuration file.
    //
    // The format consists of groups, values, and key-value pairs, where the
    // configuration file contents belong to an implicit top-level group.
    // Keys and values are strings, and need not be quoted unless they contain
    // whitespace, escape sequences, control characters or one of ",:=#[]{}()".
    // Strings that need it may be quoted using either " or '. Character
    // escaping rules are a laxer version of those defined by JSON.
    //
    // Groups contain values and/or key-value pairs (where ':' or '=' separate
    // keys from values). They are opened with '{', '[' or '(', and closed
    // with ')', ']' or '}'. Groups are mapped to command line options by
    // flattening. Values and key-value pairs may be separated by whitespace
    // or commas; trailing commas are permitted. To illustrate:
    //
    //     {a: {b:c, d,}}, e,
    //
    // and
    //
    //     a: {b:c d}, e
    //
    // and
    //
    //     a=(b=c d) e
    //
    // are all equivalent to specifying --a.b=c --a=d --e on the command line,
    // assuming that the key-separator is set to '.'.
    //
    // Comments are allowed - they begin with a '#' character, and extend to
    // the end of the line, where lines are terminated by either '\r' or
    // '\n'.
    class Parser {
    public:
        explicit Parser(fs::path const & path, char keySeparator);
        ~Parser();

        po::parsed_options const parse(po::options_description const & desc);

    private:
        Parser(Parser const &);
        Parser & operator=(Parser const &);

        char * _data;
        char const * _beg;
        char const * _end;
        char _sep;

        string const _join(vector<string> const & keys);
        string const _parseValue();
        string const _parseQuotedValue(char const quote);
        void _parseUnicodeEscape(string & val);

        void _eatWhitespace() {
            for (; _beg < _end; ++_beg) {
                char c = *_beg;
                if (c != '\t' && c != '\n' && c != '\r' && c != ' ') {
                    break;
                }
            }
        }
        void _eatLine() {
            for (; _beg < _end; ++_beg) {
                char c = *_beg;
                if (c == '\r' || c == '\n') {
                    break;
                }
            }
        }
    };

    Parser::Parser(fs::path const & path, char keySeparator) :
        _data(0), _beg(0), _end(0), _sep(keySeparator)
    {
        lsst::qserv::admin::dupr::InputFile f(path);
        size_t sz = static_cast<size_t>(f.size());
        _data = static_cast<char *>(malloc(sz));
        if (!_data) {
            throw bad_alloc();
        }
        _beg = _data;
        _end = _data + sz;
        f.read(_data, 0, sz);
    }

    Parser::~Parser() {
        free(_data);
        _data = 0;
        _beg = 0;
        _end = 0;
    }

    string const Parser::_join(vector<string> const & keys) {
        typedef vector<string>::const_iterator Iter;
        string key;
        for (Iter k = keys.begin(), ke = keys.end(); k != ke; ++k) {
            if (k->empty()) {
                continue;
            }
            size_t i = k->find_first_not_of(_sep);
            if (i == string::npos) {
                continue;
            }
            size_t j = k->find_last_not_of(_sep);
            if (!key.empty()) {
                key.push_back(_sep);
            }
            key.append(*k, i, j - i + 1);
        }
        return key;
    }

    string const Parser::_parseValue() {
        string val;
        while (_beg < _end) {
            char const c = *_beg;
            switch (c) {
                case '\t': case '\n': case '\r': case ' ':
                case '#': case ',': case ':': case '=':
                case '(': case ')': case '[': case ']': case '{': case '}':
                    return val;
                default:
                    if (c < 0x20) {
                        throw runtime_error("Unquoted values must not "
                                            "contain control characters.");
                    }
                    break;
            }
            val.push_back(c);
            ++_beg;
        }
        return val;
    }

    void Parser::_parseUnicodeEscape(string & value) {
        unsigned int cp = 0;
        int i = 0;
        // Extract 1-4 hexadecimal digits to build a Unicode
        // code-point in the Basic Multilingual Plane.
        for (; i < 4 && _beg < _end; ++i, ++_beg) {
            char c = *_beg;
            if (c >= '0' && c <= '9') {
                cp = (cp << 4) + static_cast<unsigned int>(c - '0');
            } else if (c >= 'a' && c <= 'f') {
                cp = (cp << 4) + static_cast<unsigned int>(c - 'a') + 10u;
            } else if (c >= 'A' && c <= 'F') {
                cp = (cp << 4) + static_cast<unsigned int>(c - 'A') + 10u;
            } else {
                break;
            }
        }
        if (i == 0) {
            throw runtime_error("Invalid unicode escape in quoted value");
        }
        // UTF-8 encode the code-point.
        if (cp <= 0x7f) {
            // 0xxxxxxx
            value.push_back(static_cast<char>(cp));
        } else if (cp <= 0x7ff) {
            // 110xxxxx 10xxxxxx
            value.push_back(static_cast<char>(0xc0 | (cp >> 6)));
            value.push_back(static_cast<char>(0x80 | (cp & 0x3f)));
        } else {
            assert(cp <= 0xffff);
            // 1110xxxx 10xxxxxx 10xxxxxx
            value.push_back(static_cast<char>(0xe0 | (cp >> 12)));
            value.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3f)));
            value.push_back(static_cast<char>(0x80 | (cp & 0x3f)));
        }
    }

    string const Parser::_parseQuotedValue(char const quote) {
        string val;
        while (true) {
            if (_beg >= _end) {
                throw runtime_error("Unmatched quote character.");
            }
            char c = *_beg;
            if (c == quote) {
                ++_beg;
                break;
            } else if (c == '\\') {
                // Handle escape sequence.
                ++_beg;
                if (_beg == _end) {
                    throw runtime_error("Unmatched quote character.");
                }
                c = *_beg;
                switch (c) {
                    case 'b': c = '\b'; break;
                    case 'f': c = '\f'; break;
                    case 'n': c = '\n'; break;
                    case 'r': c = '\r'; break;
                    case 't': c = '\t'; break;
                    case 'u':
                        ++_beg;
                        _parseUnicodeEscape(val);
                        continue;
                    default:
                        break;
                }
            }
            val.push_back(c);
            ++_beg;
        }
        return val;
    }

    po::parsed_options const Parser::parse(po::options_description const & desc) {
        set<string> registered;
        for (vector<shared_ptr<po::option_description> >::const_iterator i =
             desc.options().begin(), e = desc.options().end(); i != e; ++i) {
            if ((*i)->long_name().empty()) {
                throw logic_error(string("Abbreviated option names are not "
                                         "allowed in configuration files."));
            }
            registered.insert((*i)->long_name());
        }
        po::parsed_options parsed(&desc);
        po::option opt;
        vector<string> keys;
        vector<pair<int, char> > groups;
        pair<int, char> p(0, '\0');
        groups.push_back(p);
        int lvl = 0;
        for (_eatWhitespace(); _beg < _end; _eatWhitespace()) {
            char c = *_beg;
            string s;
            switch (c) {
                case '#':
                    ++_beg;
                    _eatLine();
                    continue;
                case ',':
                    ++_beg;
                    continue;
                case '(': case '[': case '{':
                    ++_beg;
                    groups.push_back(pair<int, char>(lvl, c));
                    continue;
                case ')': case ']': case '}':
                    ++_beg;
                    p = groups.back();
                    groups.pop_back();
                    if (p.second == '(' && c != ')') {
                        throw runtime_error("Unmatched (.");
                    } else if (p.second == '[' && c != ']') {
                        throw runtime_error("Unmatched [.");
                    } else if (p.second == '{' && c != '}') {
                        throw runtime_error("Unmatched {.");
                    } else if (p.second == '\0') {
                        throw runtime_error("Unmatched ), ], or }.");
                    }
                    for (; lvl > groups.back().first; --lvl) {
                        keys.pop_back();
                    }
                    continue;
                case '"': case '\'':
                    ++_beg;
                    s = _parseQuotedValue(c);
                    break;
                default:
                    s = _parseValue();
                    break;
            }
            _eatWhitespace();
            c = (_beg < _end) ? *_beg : ',';
            if (c == ':' || c == '=') {
                ++_beg;
                keys.push_back(s);
                ++lvl;
                continue;
            }
            opt.value.clear();
            opt.original_tokens.clear(); 
            if (keys.empty()) {
                opt.string_key = s;
            } else {
                opt.string_key = _join(keys);
                opt.value.push_back(s);
                opt.original_tokens.push_back(opt.string_key);
            }
            opt.unregistered =
                (registered.find(opt.string_key) == registered.end());
            opt.original_tokens.push_back(s);
            for (; lvl > groups.back().first; --lvl) {
                keys.pop_back();
            }
            parsed.options.push_back(opt);
        }
        if (!keys.empty() || lvl != 0 || groups.size() != 1u) {
            throw runtime_error("Missing value for key, or unmatched (, [ or {.");
        }
        return parsed;
    }

} // unnamed namespace


namespace lsst { namespace qserv { namespace admin { namespace dupr {

void parseCommandLine(po::variables_map & vm,
                      po::options_description const & options,
                      int argc,
                      char const * const * argv,
                      char const * help)
{
    // Define common options.
    po::options_description common("\\_____________________ Common", 80);
    common.add_options()
        ("help,h",
         "Demystify program usage.")
        ("verbose,v",
         "Chatty output.")
        ("config-file,c", po::value<vector<string> >(),
         "The name of a configuration file containing program option values "
         "in a JSON-like format. May be specified any number of times. If an "
         "option is specified more than once, the first specification "
         "usually takes precedence. Command line options have the highest "
         "precedence, followed by configuration files, which are parsed in "
         "the order specified on the command-line. Configuration files cannot "
         "currently reference other configuration files.");
    po::options_description all;
    all.add(common).add(options);
    // Parse command line.
    po::store(po::parse_command_line(argc, argv, all), vm);
    po::notify(vm);
    if (vm.count("help") != 0) {
        cout << argv[0]  << " [options]\n\n" << help << "\n" << all << endl;
        exit(EXIT_SUCCESS);
    }
    // Parse configuration files, if any.
    if (vm.count("config-file") != 0) {
        typedef vector<string>::const_iterator Iter;
        vector<string> files = vm["config-file"].as<vector<string> >();
        for (Iter f = files.begin(), e = files.end(); f != e; ++f) {
            Parser p(fs::path(*f), '.');
            po::store(p.parse(options), vm);
            po::notify(vm);
        }
    }
}

}}}} // namespace lsst::qserv::admin::dupr
