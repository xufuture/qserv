FROM fjammes/qserv:latest
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

RUN apt-get update && apt-get -y install byobu git vim

COPY scripts/*.sh /home/qserv/scripts/
RUN chown -R qserv:qserv /home/qserv/scripts

USER qserv 

WORKDIR /home/qserv
RUN scripts/build.sh u/fjammes/DM-3986
