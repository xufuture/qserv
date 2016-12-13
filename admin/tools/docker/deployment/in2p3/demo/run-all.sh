OUT_DIR=~/src/asterics-demo/dev
./short-queries.sh >& $OUT_DIR/short.out
./count-queries.sh >& $OUT_DIR/count.out
./long-queries.sh >& $OUT_DIR/long.out
./sscan-queries.sh >& $OUT_DIR/sscan.out
