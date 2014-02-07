
#ifndef LSST_CSS_C_HH
#define LSST_CSS_C_HH

// standard library imports
#include <vector>
#include <string>

// third-party imports
#include "zookeeper.h"

class CssInterface {
public:
    CssInterface(std::string const& connInfo);
    ~CssInterface();

    void create(std::string const& key, std::string const& value);
    std::string exists(std::string const& key);
    std::string get(std::string const& key);
    std::vector<std::string> getChildren(std::string const& key);
    void deleteNode(std::string const& key, bool recurvive);

private:
    zhandle_t *_zh; // zookeeper handle
};

#endif // LSST_CSS_C_HH
