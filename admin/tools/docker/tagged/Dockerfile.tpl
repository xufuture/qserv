FROM fjammes/qserv:latest
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

RUN apt-get update && apt-get -y install byobu git vim

USER qserv 

WORKDIR /home/qserv

# Update to latest qserv dependencies, need to be publish on distribution server
# see https://confluence.lsstcorp.org/display/DM/Qserv+Release+Procedure
RUN bash -c ". /qserv/stack/loadLSST.bash && eups distrib install qserv_distrib -t qserv-dev --onlydepend -vvv"

RUN mkdir src && cd src && git clone git://github.com/LSST/qserv
WORKDIR /home/qserv/src/qserv
RUN git checkout {{GIT_TAG_OPT}} 

RUN bash -c ". /qserv/stack/loadLSST.bash && setup -r . -t qserv-dev && eupspkg -er install"
RUN bash -c ". /qserv/stack/loadLSST.bash && eupspkg -er install -t qserv_latest"
