#include "lsst/qserv/worker/XrootFilename.h"
#include <sstream>

namespace qWorker = lsst::qserv::worker;
////////////////////////////////////////////////////////////////////////
// XrootFilename::TokenAcceptor
////////////////////////////////////////////////////////////////////////
class qWorker::XrootFilename::TokenAcceptor {
public:
    typedef std::map<std::string, std::string> Map;
    explicit TokenAcceptor(Map& m) : map(m) {}
    void operator()(std::string const& s) {
        Size p = s.find('=');
        std::string v;
        std::string k;
        if(p != std::string::npos) {
            v = s.substr(p+1);
        }
        map[s.substr(0, p)] = v;        
    }
    Map& map;
};
////////////////////////////////////////////////////////////////////////
// XrootFilename
////////////////////////////////////////////////////////////////////////
void qWorker::XrootFilename::_makeMap(std::string const& queryString) {
    char const pairSep = '&';
    TokenAcceptor ta(_map);
    _tokenize(queryString, pairSep, ta); 
}

void qWorker::XrootFilename::addValue(std::string const& k, 
                                      std::string const& value) {
    _map[k] = value;
    _updateString();
}

void qWorker::XrootFilename::_updateString() {
    Map::const_iterator i;
    std::stringstream ss;
    bool needSep = false;
    ss << getFile() << "?";
    for(i=_map.begin(); i != _map.end(); ++i) {
        if(needSep) ss << "&";
        ss << i->first << "=" << i->second;
        needSep = true;
    }
    _original = ss.str();
    _findSplit();
}

void qWorker::XrootFilename::_findSplit() {
    _splitPos = _original.find('?');
}
