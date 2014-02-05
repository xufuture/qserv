
// Quick and dirty test, based on:
// http://zookeeper.apache.org/doc/r3.3.4/zookeeperProgrammers.html#ZooKeeper+C+client+API
//
// To build:
//   gcc css/c.cc -I <distDir>/include/zookeeper -L <distDir>/lib -l zookeeper_mt -o testZooC
//   export LD_LIBRARY_PATH=<distDir>/lib


#include <string.h>
#include <errno.h>
#include <unistd.h>
#include "zookeeper.h"

static zhandle_t *zh;

/** Watcher function -- empty for this example, not something you should
 * do in real code */
void watcher(zhandle_t *zzh, int type, int state, const char *path,
             void *watcherCtx) {}


int main(int argc, char** argv) {
    char buffer[512];
    char p[2048];
    char appId[64];

    strcpy(appId, "example.foo_test");

    zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);

    zh = zookeeper_init("localhost:2181", watcher, 10000, 0, 0, 0);
    if (!zh) {
        return errno;
    }

    int rc = zoo_create(zh,"/xyz", "value", 5, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL,
                        buffer, sizeof(buffer)-1);
    if (rc) {
        fprintf(stderr, "zoo_create failed, error %d\n", rc);
    }

    int buflen= sizeof(buffer);
    struct Stat stat;
    rc = zoo_get(zh, "/xyz", 0, buffer, &buflen, &stat);
    if (rc) {
        fprintf(stderr, "zoo_get failed, error %d\n", rc);
    }
    sleep(10);
    

    zookeeper_close(zh);
    return 0;
}
