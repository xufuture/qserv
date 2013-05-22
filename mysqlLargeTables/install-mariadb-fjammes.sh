sudo yum-builddep mysql
sudo yum install cmake

cf. 
https://kb.askmonty.org/en/running-mariadb-from-the-source-directory/

Créer un fichier my.cnf

sudo mkdir /data/LSST/mysql
sudo chown fjammes:fjammes /data/LSST/mysql

su cp /etc/my.cnf /etc/my.cnf.0 
et supprimer la ligne "user=mysql"

/home/fjammes/src/mariadb-5.5.30/scripts/mysql_install_db --srcdir=/home/fjammes/src/mariadb-5.5.30/ --datadir=/data/LSST/mysql/ --user=$LOGNAME
/usr/local/mysql/bin/mysqld_safe&
/usr/local/mysql/bin/mysql -u root < ~/W13-params.sql


# DATA LOADING

nohup ./load-data.sh &

# RESTART
killall mysqld_safe mysqld mysql load-data.sh; rm -rf /data/LSST/mysql/*

# VIEW
cat nohup.out ; du -skh /data/LSST/mysql/w13/*
