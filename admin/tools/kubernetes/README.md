# Install

See http://kubernetes.io/docs/getting-started-guides/openstack-heat/#starting-a-cluster

cd kubernetes
# Get kubernetes and start cluster
# export KUBERNETES_PROVIDER=openstack-heat; curl -sS https://get.k8s.io | bash
KUBERNETES_PROVIDER=openstack-heat ./cluster/kube-up.sh

# Log to master

ssh -i /home/qserv/.ssh/id_rsa_openstack -vvv minion@141.142.210.250

# Remove stack

 openstack stack delete KubernetesStack
