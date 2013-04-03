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

#include <stdio.h>
#include <string.h>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE MapReduce
#include "boost/test/unit_test.hpp"

#include "Csv.h"
#include "FileUtils.h"
#include "MapReduce.h"
#include "TempFile.h"

namespace po = boost::program_options;
namespace dupr = lsst::qserv::admin::dupr;

using std::string;
using boost::shared_ptr;

namespace {

    unsigned int const NUM_LINES = 1024*1024;

    /// Generate CSV files containing a total of NUM_LINES lines, where
    /// each line consists of a single line number.
    void buildInput(TempFile const & t1, TempFile const & t2) {
        char buf[17];
        unsigned int line;
        dupr::BufferedAppender a(1*dupr::MiB);
        a.open(t1.path(), true);
        for (line = 0; line < NUM_LINES/3; ++line) {
            snprintf(buf, sizeof(buf), "%15u\n", NUM_LINES - 1 - line);
            a.append(buf, strlen(buf));
        }
        a.open(t2.path(), true);
        for (; line < NUM_LINES; ++line) {
            snprintf(buf, sizeof(buf), "%15u\n", NUM_LINES - 1 - line);
            a.append(buf, strlen(buf));
        }
        a.close();
    }

    // Map-reduce key: a line number. 
    struct Key {
        uint32_t line;
        uint32_t hash() const { return line; }
        bool operator<(Key const & k) const { return line < k.line; }
    };

    /// 1-bit per line number, tracking whether a line was processed or not.
    class Lines {
    public:
        Lines() : _bits(NUM_LINES, false), _failed(false) { }
        ~Lines() { }

        void mark(uint32_t line) {
            if (_bits[line]) { _failed = true; }
            _bits[line] = true;
        }

        void merge(Lines const & lines) {
            for (size_t i = 0; i < NUM_LINES; ++i) {
                if (lines._bits[i]) {
                    if (_bits[i]) { _failed = true; }
                    _bits[i] = true;
                }
            }
        }

        void verify() {
            for (size_t i = 0; i < NUM_LINES; ++i) {
                if (!_bits[i]) { _failed = true; }
            }
            BOOST_CHECK(!_failed);
        }

    private:
        std::vector<bool> _bits;
        bool _failed;
    };
    
    class Worker : public dupr::WorkerBase<Key, Lines> {
    public:
        Worker(po::variables_map const & vm) :
            _editor(vm), _lines(new Lines()) { }

        void map(char const * beg, char const * end, Silo & silo) {
            Key k;
            while (beg < end) {
                beg = _editor.readRecord(beg, end);
                k.line = _editor.get<uint32_t>(0);
                silo.add(k, _editor);
            }
        }

        void reduce(RecordIter beg, RecordIter end) {
            for (; beg != end; ++beg) {
                _lines->mark(beg->key.line);
            }
        }

        void finish() { }

        shared_ptr<Lines> const result() { return _lines; }

        static void defineOptions(po::options_description & opts) {
            dupr::csv::Editor::defineOptions(opts);
        }

    private:
        dupr::csv::Editor _editor;
        shared_ptr<Lines> _lines;
    };

    typedef dupr::Job<Worker> TestJob;

} // unnamed namespace


BOOST_AUTO_TEST_CASE(MapReduceTest) {
    char const * argv[7] = {
        "dummy",
        "--in.csv.field=line",
        "--mr.block-size=1",
        "--mr.pool-size=8",
        0,
        0,
        0
    };
    TempFile t1, t2;
    buildInput(t1, t2);
    string const s1("--in=" + t1.path().native());
    string const s2("--in=" + t2.path().native());
    argv[4] = s1.c_str();
    argv[5] = s2.c_str();
    po::options_description options;
    TestJob::defineOptions(options);
    for (char n = '1'; n < '8'; ++n) {
        string s("--mr.num-workers="); s += n;
        argv[6] = s.c_str();
        po::variables_map vm;
        po::store(po::parse_command_line(7, argv, options), vm);
        po::notify(vm);
        TestJob job(vm);
        shared_ptr<Lines> lines = job.run();
        lines->verify();
    }
}
