FROM {{DOCKER_IMAGE_OPT}}
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

WORKDIR /qserv

COPY scripts/*.sh scripts/

USER root
RUN chown -R qserv:qserv /qserv/scripts/

USER qserv

RUN /qserv/scripts/configure.sh {{NODE_TYPE_OPT}} {{MASTER_FQDN_OPT}}

# WARNING: Unsafe because it is pushed in Docker Hub
# TODO: use consul to manage secret
COPY wmgr.secret /qserv/run/etc/

# 'tail -F' allow container not to exit
CMD /qserv/run/bin/qserv-start.sh && tail -F /qserv/run/var/log/worker/xrootd.log
