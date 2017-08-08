FROM mariadb:10.1.25
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

# Start with this long step not to re-run it on
# each Dockerfile update
RUN apt-get -y update && \
    apt-get -y upgrade && \
    apt-get -y clean

RUN apt-get -y install git python gcc
RUN apt-get -y install libmariadbclient-dev make

RUN usermod -u 1000 mysql && \
    groupmod -g 1000 mysql

RUN cd /usr/local/src && \
    git clone https://github.com/smonkewitz/scisql.git && \
    cd scisql && \
    ./configure --mysql-includes=/usr/include/mysql && \
    make && \
    make install

