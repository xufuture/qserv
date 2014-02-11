
// Test program for CssInterface.cc
//
// To build: 
// gcc -c tests/testCssInterface.cc -I . -I <distDir>/include/zookeeper -o tests/testCssInterface.o
//   export LD_LIBRARY_PATH=<distDir>/lib


// standard library imports
#include <iostream>
#include <stdexcept>
#include <string.h> // for memset

#include "cssInterface.h"

using std::cout;
using std::endl;
using std::string;
using std::vector;

int main(int argc, char* argv[]) {
    try {    
        string k1 = "/xyzA";
        string k2 = "/xyzB";
        string k3 = "/xyzC";
        string v1 = "firstOne";
        string v2 = "secondOne";

        CssInterface cssI = CssInterface("localhost:2181");

        cssI.create(k1, v1);
        cssI.create(k2, v2);

        string s = cssI.get(k1);
        cout << "got " << s << " for key " << k1 << endl;

        cout << "checking exist for " << k1 << endl;
        cout << "got: " << cssI.exists(k1) << endl;

        cout << "checking exist for " << k3 << endl;
        cout << "got: " << cssI.exists(k3) << endl;

        std::vector<string> v = cssI.getChildren("/");
        cout << "children of '/': ";
        for (std::vector<string>::iterator it = v.begin() ; it != v.end(); ++it) {
            cout << *it << " ";
        }
        cout << endl;

        cssI.deleteNode(k1);
        cssI.deleteNode(k2);

        v = cssI.getChildren("/");
        cout << "children of '/': ";
        for (std::vector<string>::iterator it = v.begin() ; it != v.end(); ++it) {
            cout << *it << " ";
        }
        cout << endl;
    } catch (std::runtime_error &ex) {
        cout << "Cought exception: " << ex.what() << endl;
        return -1;
    } 
    return 0;
}
