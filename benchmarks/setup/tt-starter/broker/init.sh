#!/usr/bin/env bash
set -eux -o pipefail

export ZEEBE_HOST=$(hostname -f)
export ZEEBE_NODE_ID=${ZEEBE_NODE_NAME##*-}

# As the number of replicas or the DNS is not obtainable from the downward API yet,
# defined them here based on conventions
replicaHost=${ZEEBE_NODE_NAME%-*}
export ZEEBE_CONTACT_POINTS=$(for ((i=0; i<${ZEEBE_CLUSTER_SIZE}; i++)); do echo -n "${replicaHost}-$i.$(hostname -d):26502,"; done)

exec tini -- /usr/local/bin/startup.sh
