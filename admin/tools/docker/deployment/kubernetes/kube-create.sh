#!/bin/bash

# Create Kubernetes cluster

# @author Fabrice Jammes SLAC/IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"

SSH_CFG="$DIR/ssh_config"

echo "Create Kubernetes cluster"
ssh -t -F "$SSH_CFG" "$SWARM_NODE" "sudo systemctl start kubelet.service"
OUTPUT=$(ssh -t -F "$SSH_CFG" "$SWARM_NODE" "sudo kubeadm init")

JOIN_CMD=$(echo "$OUTPUT" | tail -n 1 | sed "s/\r//")

# Join Kubernetes nodes:
#   - Qserv master has index 0
#   - QServ workers have indexes >= 1
for qserv_node in $MASTER $WORKERS
do
    echo "Join $qserv_node to Kubernetes cluster"
    # FIXME: cleanup below is a workaroung, open ticket on kubernetes github
	ssh -t -F "$SSH_CFG" "$qserv_node" "sudo systemctl stop kubelet && \
       sudo systemctl stop docker && \
       sudo rm -rf /etc/kubernetes/* /var/lib/kubelet/pods \
           /var/lib/kubelet/plugins && \
       sudo systemctl start docker && \
       sudo systemctl start kubelet"
	ssh -t -F "$SSH_CFG" "$qserv_node" "sudo $JOIN_CMD"
done

ssh -t -F "$SSH_CFG" "$SWARM_NODE" "kubectl apply -f https://git.io/weave-kube"
