#!/bin/bash

# Count objects on all Qserv workers

# Launch using parallel
cp count.sh /tmp/count.sh
RESULTS=$(echo /tmp/count.sh | parallel --onall --slf qserv.slf "sh -c '{}'")

i=0
for r in $RESULTS
do
    if [ $i -eq 0 ]; then
        ADD="$r"
    else
        ADD="${ADD}+$r"
    fi
    i=$((i+1))
done

echo "$ADD" | bc
