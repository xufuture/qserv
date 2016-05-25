#!/usr/bin/env python

"""
<<<<<<< HEAD
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
=======
Boot instances in cluster infrastructure, and use cloud config to create users
and install packages on virtual machines

Script performs these tasks:
- launch instances from image and manage ssh key
- create gateway vm
- check for available floating ip adress
- add it to gateway
- cloud config

@author  Oualid Achbal, ISIMA student , IN2P3
>>>>>>> 02dbadc... Implement Swarm POC on Openstack

"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
<<<<<<< HEAD
import argparse
import logging
import os
import sys
=======
import logging
import os
import re
import subprocess
import sys
import time
import warnings
>>>>>>> 02dbadc... Implement Swarm POC on Openstack

# ----------------------------
# Imports for other modules --
# ----------------------------
<<<<<<< HEAD
from novaclient.exceptions import BadRequest
import cloudmanager
=======
from novaclient import client
import novaclient.exceptions
>>>>>>> 02dbadc... Implement Swarm POC on Openstack

# -----------------------
# Exported definitions --
# -----------------------
<<<<<<< HEAD
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

    cloudManager.print_ssh_config(instances, floating_ip)

    # Wait for cloud config completion for all machines
    for instance in instances:
        cloudManager.detect_end_cloud_config(instance)

    cloudManager.check_ssh_up(instances)

    cloudManager.update_etc_hosts(instances)

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
=======
def get_nova_creds():
    """
    Extract the login information from the environment
    """
    d = {}
    d['version'] = 2
    d['username'] = os.environ['OS_USERNAME']
    d['api_key'] = os.environ['OS_PASSWORD']
    d['auth_url'] = os.environ['OS_AUTH_URL']
    d['project_id'] = os.environ['OS_TENANT_NAME']
    d['insecure'] = True
    logging.debug("Openstack user: {}".format(d['username']))
    return d

def manage_ssh_key():
    """
    Upload ssh public key
    """
    logging.info('Manage ssh keys: {}'.format(key))
    if nova.keypairs.findall(name=key):
        logging.debug('Remove previous ssh keys')
        nova.keypairs.delete(key=key)

    with fpubkey:
        nova.keypairs.create(name=key, public_key=public_key)

def nova_servers_create(instance_id):
    """
    Boot an instance from an image and check status
    """
    username = creds['username'].replace('.', '')
    instance_name = "{0}-qserv-{1}".format(username, instance_id)
    logging.info("Launch an instance {}".format(instance_name))

    # cloud config
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
      # Allow docker to start via cloud-init, see https://github.com/projectatomic/docker-storage-setup/issues/77
      - [ sed, -i, -e, 's/After=cloud-final.service/# After=cloud-final.service/g', /usr/lib/systemd/system/docker-storage-setup.service]
      # docker-storage-driver fails using default setup
      - [ sed, -i, -e, '$a STORAGE_DRIVER=overlay', /etc/sysconfig/docker-storage-setup ]
      # overlay and selinux are not compliant in docker 1.9
      # label is used by swarm
      - [ sed, -i, -e, "s/OPTIONS='--selinux-enabled'/# OPTIONS='--selinux-enabled'/", /etc/sysconfig/docker ]
      - [ sed, -i, -e, 's%DOCKER_NETWORK_OPTIONS=%DOCKER_NETWORK_OPTIONS=-H unix:///var/run/docker.sock -H tcp://0.0.0.0:2375%g', /etc/sysconfig/docker-network]
      - [ /bin/systemctl, daemon-reload]
      - [ /bin/systemctl, restart,  docker.service]
    '''

    userdata = cloud_config_tpl.format(key=public_key, instance_name=instance_name)

    # Launch an instance from an image
    instance = nova.servers.create(name=instance_name, image=image,
            flavor=flavor, userdata=userdata, key_name=key, nics=nics)
    # Poll at 5 second intervals, until the status is no longer 'BUILD'
    status = instance.status
    while status == 'BUILD':
        time.sleep(5)
        instance.get()
        status = instance.status
    logging.info ("status: {}".format(status))
    logging.info ("Instance {} is active".format(instance_name))

    return instance

def nova_swarm_server_create():
    """
    Boot an instance from an image and check status
    """
    username = creds['username'].replace('.', '')
    instance_name = "{0}-swarm".format(username)
    logging.info("Launch an instance {}".format(instance_name))

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

        SWARM_HOSTNAME="{username}-swarm"
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

    userdata = cloud_config_tpl.format(branch=branch,
                                       instance_last_id=instance_number-1,
                                       instance_name=instance_name,
                                       key=public_key,
                                       username=username)

    # Launch an instance from an image
    instance = nova.servers.create(name=instance_name, image=image,
            flavor=flavor, userdata=userdata, key_name=key, nics=nics)
    # Poll at 5 second intervals, until the status is no longer 'BUILD'
    status = instance.status
    while status == 'BUILD':
        time.sleep(5)
        instance.get()
        status = instance.status
    logging.info ("status: {}".format(status))
    logging.info ("Instance {} is active".format(instance_name))

    return instance

def get_floating_ip():
    """
    Allocate floating ip to project
    """
    i=0
    floating_ips = nova.floating_ips.list()
    floating_ip = None
    floating_ip_pool = nova.floating_ip_pools.list()[0].name

    # Check for available public ip in project
    while i<len(floating_ips) and floating_ip is None:
        if floating_ips[i].instance_id is None:
            floating_ip=floating_ips[i]
            logging.debug('Available floating ip found {}'.format(floating_ip))
        i+=1

    # Check for available public ip in ext-net pool
    if floating_ip is None:
        try:
            logging.debug("Use floating ip pool: {}".format(floating_ip_pool))
            floating_ip = nova.floating_ips.create(floating_ip_pool)
        except novaclient.exceptions.Forbidden as e:
            logging.fatal("Unable to retrieve public IP: {0}".format(e))
            sys.exit(1)

    return floating_ip

def nova_servers_delete(vm_name):
    """
    Retrieve an instance by name and shut it down
    """
    server = nova.servers.find(name=vm_name)
    server.delete()

def print_ssh_config(instances, floating_ip):
    """
    Print ssh client configuration to file
    """
    # ssh config
    ssh_config_tpl = '''

    Host {host}
    HostName {fixed_ip}
    User qserv
    Port 22
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    PasswordAuthentication no
    ProxyCommand ssh -i {key_filename} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -W %h:%p qserv@{floating_ip}
    IdentityFile {key_filename}
    IdentitiesOnly yes
    LogLevel FATAL
    '''

    ssh_config_extract = ""
    for instance in instances:
        fixed_ip = instance.networks[network_name][0]
        ssh_config_extract += ssh_config_tpl.format(host=instance.name,
                                                    fixed_ip=fixed_ip,
                                                    floating_ip=floating_ip.ip,
                                                    key_filename=key_filename)

    logging.debug("SSH client config: ")

    f = open("ssh_config", "w")
    f.write(ssh_config_extract)
    f.close()

def check_ssh_up(instances):

    for instance in instances:
        cmd=['ssh', '-t', '-F', './ssh_config', instance.name, 'true']
        success = False
        while not success:
            try:
                subprocess.check_output(cmd)
                success = True
            except subprocess.CalledProcessError as exc:
                logging.warn("Waiting for ssh to be avalaible on {}: {}".format(instance.name, exc))
        logging.debug("ssh available on {}".format(instance.name))

def update_etc_hosts():

    hostfile_tpl = "{ip}    {host}\n"

    hostfile=""
    for instance in instances:
        # Collect IP adresses
        fixed_ip = instance.networks[network_name][0]
        hostfile += hostfile_tpl.format(host=instance.name, ip=fixed_ip)

    #logging.debug("hostfile.txt:\n---\n{}\n---".format(hostfile))

    # Update /etc/hosts on each machine
    for instance in instances:
        cmd=['ssh', '-t', '-F', './ssh_config', instance.name,
             'sudo sh -c "echo \'{hostfile}\' >> /etc/hosts"'.format(hostfile=hostfile)]
        #logging.debug("cmd:\n---\n{}\n---".format(cmd))
        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            logging.error("Error while updating host file: {}".format(e.output))
            sys.exit(1)

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

def detect_end_cloud_config(instance):
    # Add clean wait for cloud-init completion
    checkConfig = "---SYSTEM READY FOR SNAPSHOT---"
    is_finished = False
    while not is_finished:
        time.sleep(5)
        output = instance.get_console_output()
        logging.debug("output: {}".format(output))
        logging.debug("instance: {}".format(instance.name))
        word = re.search(checkConfig, output)
        if word != None:
            is_finished = True

if __name__ == "__main__":
    try:
        logging.basicConfig(format='%(asctime)s %(levelname)-8s %(name)-15s %(message)s',level=logging.DEBUG)

        # Disable request package logger
        logging.getLogger("requests").setLevel(logging.ERROR)
        logging.getLogger("urllib3").setLevel(logging.ERROR)

        # Disable warnings
        warnings.filterwarnings("ignore")

        creds = get_nova_creds()
        nova = client.Client(**creds)

        key_filename = '~/.ssh/id_rsa_qserv'

        # Upload ssh public key
        key = "{}-qserv".format(creds['username'])
        # Remove unsafe characters
        key = key.replace('.', '')
        fpubkey = open(os.path.expanduser(key_filename+".pub"))
        public_key=fpubkey.read()
        manage_ssh_key()

        # Find a floating ip for gateway
        floating_ip = get_floating_ip()
        if not floating_ip:
            logging.fatal("Unable to add public ip to Qserv gateway")
            sys.exit(2)

        # Find an image and a flavor to launch an instance

        nics = []
        image_name = "centos-7-qserv"
        instance_number = 3

        # CC-IN2P3
        # image_name = "CentOS-7-x86_64-GenericCloud"
        # flavor_name = "m1.medium"
        # network_name = "lsst"

        # Petasky
        # image_name = "CentOS 7"
        # flavor_name = "c1.medium"
        # network_name = "petasky-net"

        # NCSA
        flavor_name = "m1.medium"
        network_name = "LSST-net"
        nics = [ { 'net-id': u'fc77a88d-a9fb-47bb-a65d-39d1be7a7174' } ]
        ssh_security_group = "Remote SSH"

        image = nova.images.find(name=image_name)
        flavor = nova.flavors.find(name=flavor_name)

        # Create instances list
        instances = []

        # Create gateway instance and add floating_ip to it
        gateway_id = 0
        gateway_instance = nova_servers_create(gateway_id)
        logging.info("Add floating ip ({0}) to {1}".format(floating_ip,
            gateway_instance.name))
        gateway_instance.add_floating_ip(floating_ip)

        if ssh_security_group:
            gateway_instance.add_security_group(ssh_security_group)

        # Add gateway to instances list
        instances.append(gateway_instance)

        # Create worker instances
        for instance_id in range(1, instance_number):
            worker_instance = nova_servers_create(instance_id)
            # Add workers to instances list
            instances.append(worker_instance)

        swarm_instance = nova_swarm_server_create()
        instances.append(swarm_instance)

        # Show ssh client config
        print_ssh_config(instances, floating_ip)

        # Modify /etc/hosts on each machine
        for instance in instances:
            detect_end_cloud_config(instance)

        check_ssh_up(instances)

        update_etc_hosts()

        launch_integration_tests(swarm_instance)

        #for instance in instances:
        #    nova_servers_delete(instance.name)

        logging.debug("SUCCESS: Qserv Openstack cluster is up")
    except Exception as exc:
        logging.critical('Exception occured: %s', exc, exc_info=True)
        sys.exit(3)
>>>>>>> 02dbadc... Implement Swarm POC on Openstack
