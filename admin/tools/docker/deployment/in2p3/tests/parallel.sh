#!/bin/bash
set -x
# Count objects on all Qserv workers

# Launch using parallel
cp count.sh /tmp/count.sh
RESULTS=$(echo /tmp/count.sh | parallel --files --onall --slf qserv.slf "sh -c '{}'")
