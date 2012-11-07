
## Build script for monetdb libs
INSTALL_DIR=/scratch/danielw/MonetDB-Apr2012-SP1

src=monetdbtest.cc
obj=${src/%.cc/.o}
out=${src%%.cc}
src2=MonetConnection.cc
obj2=${src2/%.cc/.o}

function monetCompile {
    local src=$1
    local obj=${src/%.cc/.o}
    libtool --mode=compile --tag=CXX g++ -g \
        -c `env PKG_CONFIG_PATH=$INSTALL_DIR/lib/pkgconfig pkg-config --cflags monetdb-mapi` $src || exit 1
}

monetCompile $src
monetCompile $src2
libtool --mode=link --tag=CXX g++ -o $out `env PKG_CONFIG_PATH=$INSTALL_DIR/lib/pkgconfig pkg-config --libs monetdb-mapi` $obj $obj2


