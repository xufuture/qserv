#!/usr/bin/env python

"""
Boot instances from an image already created containing Docker
in OpenStack infrastructure, and use cloud config to create users
on virtual machines

Script performs these tasks:
  - launch instances from image and manage ssh key
  - create gateway vm
  - check for available floating ip address
  - add it to gateway
  - create users via cloud-init
  - update /etc/hosts on each VM
  - print ssh client config

@author  Oualid Achbal, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import argparse
import logging
import os
import subprocess
import sys

# ----------------------------
# Imports for other modules --
# ----------------------------
from novaclient.exceptions import BadRequest
import cloudmanager

# -----------------------
# Exported definitions --
# -----------------------

def get_swarm_cloudconfig():
    """
    Get cloudconfig configuration for a Swarm instance
    """
    username = cloudManager.get_safe_username()
    # cloud config
    cloud_config_tpl = '''
    #cloud-config

    write_files:
    - path: /tmp/swarm-env.sh
      permissions: "0754"
      owner: "root:docker"
      content: |
        #!/bin/sh

        # Cluster parameters used to run
        # swarm and docker

        # @author  Fabrice Jammes, IN2P3

        INSTANCE_LAST_ID={instance_last_id}
        HOSTNAME_TPL="{username}-qserv-"

        DOCKER_PORT=2375

        SWARM_HOSTNAME="{username}-qserv-swarm"
        SWARM_PORT=3376

    users:
    - name: qserv
      gecos: Qserv daemon
      groups: docker
      lock-passwd: true
      shell: /bin/bash
      ssh-authorized-keys:
      - {key}
      sudo: ALL=(ALL) NOPASSWD:ALL

    packages:
    - git

    runcmd:
      - [/tmp/detect_end_cloud_config.sh]
      # Allow docker to start via cloud-init, see https://github.com/projectatomic/docker-storage-setup/issues/77
      - [sed, -i, -e, 's/After=cloud-final.service/# After=cloud-final.service/g', /usr/lib/systemd/system/docker-storage-setup.service]
      # docker-storage-driver fails using default setup
      - [sed, -i, -e, '$a STORAGE_DRIVER=overlay', /etc/sysconfig/docker-storage-setup ]
      # overlay and selinux are not compliant in docker 1.9
      - [sed, -i, -e, "s/OPTIONS='--selinux-enabled'/# OPTIONS='--selinux-enabled'/", /etc/sysconfig/docker ]
      - [/bin/systemctl, daemon-reload]
      - [/bin/systemctl, restart,  docker.service]
      - [su, qserv, -c, "git clone -b {branch} --single-branch https://github.com/lsst/qserv.git /home/qserv/src/qserv"]
      - [cp, /tmp/swarm-env.sh, /home/qserv/src/qserv/admin/tools/docker/deployment/swarm]
    '''

    cmd = ['git', 'rev-parse', '--abbrev-ref', 'HEAD']
    try:
        branch = subprocess.check_output(cmd).strip()
    except subprocess.CalledProcessError:
        logging.debug("Unable to retrieve current git branch, "
                      "using qserv master branch"
                      "to retrieve swarm procedure code.")
        branch = "master"

    logging.info("Cloning swarm procedure code from "
                 "https://github.com/lsst/qserv.git (branch: {})".format(branch))

    fpubkey = open(os.path.expanduser(cloudManager.key_filename + ".pub"))
    public_key = fpubkey.read()

    userdata = cloud_config_tpl.format(branch=branch,
                                       instance_last_id=args.nbServers-1,
                                       key=public_key,
                                       username=username)


    return userdata

def launch_integration_tests(swarm_instance):

    logging.info("Launching integration tests")
    # TODO check swarm nodes registration, instead of sleeping
    cmd_tpl = ['ssh', '-F', './ssh_config', swarm_instance.name]
    cmd_1 = cmd_tpl + ['/home/qserv/src/qserv/admin/tools/docker/deployment/'
                       'swarm/swarm-setup.sh']
    cmd_2 = cmd_tpl + ['/home/qserv/src/qserv/admin/tools/docker/deployment/'
                       'swarm/run-multinode-tests.sh']

    for cmd in [cmd_1, cmd_2]:
        logging.debug("Lauch '{}' on swarm node".format(cmd))
        rc = run_command(cmd)
        if rc != 0:
            logging.error("Command '{}' returned error code: {}".format(cmd, rc))
            sys.exit(1)

def run_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    for line in iter(process.stdout.readline, ''):
        msg = line.rstrip()
        if msg:
            logging.debug("stdout: \"{}\"".format(msg))
    rc = process.poll()
    return rc

def get_cloudconfig():
    """
    Return cloud init configuration in a string
    """
    cloud_config_tpl = '''
#cloud-config
users:
- name: qserv
  gecos: Qserv daemon
  groups: docker
  lock-passwd: true
  shell: /bin/bash
  ssh-authorized-keys:
  - {key}
  sudo: ALL=(ALL) NOPASSWD:ALL

runcmd:
- ['/tmp/detect_end_cloud_config.sh']
- [mkdir, -p, '/qserv/data']
- [mkdir, -p, '/qserv/log']
- [chown, "1000:1000", '/qserv/data']
- [chown, "1000:1000", '/qserv/log']
# Allow docker to start via cloud-init
# see https://github.com/projectatomic/docker-storage-setup/issues/77
- [ sed, -i, -e, 's/After=cloud-final.service/#After=cloud-final.service/g',
  /usr/lib/systemd/system/docker-storage-setup.service]
# 'overlay' seems more robust than default setting
- [ sed, -i, -e, '$a STORAGE_DRIVER=overlay', /etc/sysconfig/docker-storage-setup ]
# overlay and selinux are not compliant in docker 1.9
- [ sed, -i, -e, "s/OPTIONS='--selinux-enabled'/# OPTIONS='--selinux-enabled'/", /etc/sysconfig/docker ]
# Open tcp socket on all nodes to enable Swarm
- [ sed, -i, -e, 's%DOCKER_NETWORK_OPTIONS=%DOCKER_NETWORK_OPTIONS=-H unix:///var/run/docker.sock -H tcp://0.0.0.0:2375%g', /etc/sysconfig/docker-network]
- [ /bin/systemctl, daemon-reload]
- [ /bin/systemctl, restart,  docker.service]
'''
    fpubkey = open(os.path.expanduser(cloudManager.key_filename + ".pub"))
    public_key=fpubkey.read()
    userdata = cloud_config_tpl.format(key=public_key)

    return userdata

def main():
    cloudManager.manage_ssh_key()

    userdata_provision = get_cloudconfig()
    userdata_swarm = get_swarm_cloudconfig()

    # Create instances list
    instances = []

    # Create gateway instance and add floating_ip to it
    gateway_id = 0
    gateway_instance = cloudManager.nova_servers_create(gateway_id,
                                                        userdata_provision)

    # Find a floating ip address for gateway
    floating_ip = cloudManager.get_floating_ip()
    if not floating_ip:
        logging.critical("Unable to add public ip to Qserv gateway")
        sys.exit(1)
    logging.info("Add floating ip ({0}) to {1}".format(floating_ip,
                                                       gateway_instance.name))
    try:
        gateway_instance.add_floating_ip(floating_ip)
    except BadRequest as exc:
        logging.critical('The procedure needs to be restarted. '
                         'Exception occurred: %s', exc)
        cloudManager.nova_servers_delete(gateway_instance)
        sys.exit(1)

    # Manage ssh security group
    if cloudManager.ssh_security_group:
        gateway_instance.add_security_group(cloudManager.ssh_security_group)

    instances.append(gateway_instance)

    # Create worker instances
    for instance_id in range(1, args.nbServers):
        worker_instance = cloudManager.nova_servers_create(instance_id,
                                                           userdata_provision)
        instances.append(worker_instance)

    instance_id = 'swarm'
    swarm_instance = cloudManager.nova_servers_create(instance_id,
                                                      userdata_swarm)
    instances.append(swarm_instance)


    cloudManager.print_ssh_config(instances, floating_ip)

    # Wait for cloud config completion for all machines
    for instance in instances:
        cloudManager.detect_end_cloud_config(instance)

    cloudManager.check_ssh_up(instances)

    cloudManager.update_etc_hosts(instances)

    launch_integration_tests(swarm_instance)

    logging.debug("SUCCESS: Qserv Openstack cluster is up")


if __name__ == "__main__":
    try:
        # Define command-line arguments
        parser = argparse.ArgumentParser(description='Boot instances from image containing Docker.')
        parser.add_argument('-n', '--nb-servers', dest='nbServers',
                           required=False, default=3, type=int,
                           help='Choose the number of servers to boot')

        cloudmanager.add_parser_args(parser)
        args = parser.parse_args()

        loggerName = "Provisioner"
        cloudmanager.config_logger(loggerName, args.verbose, args.verboseAll)

        cloudManager = cloudmanager.CloudManager(config_file_name=args.configFile, used_image_key=cloudmanager.SNAPSHOT_IMAGE_KEY, add_ssh_key=True)

        main()
    except Exception as exc:
        logging.critical('Exception occurred: %s', exc, exc_info=True)
        sys.exit(1)
