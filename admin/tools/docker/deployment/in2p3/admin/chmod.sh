set -x

echo $PASSWORD

/opt/shmux/bin/shmux -c "echo \"$PASSWORD\" | sudo -S sh -c 'chmod o+r \
    /qserv/data/mysql/aria_log.00000001 \
    /qserv/data/mysql/aria_log_control \
    /qserv/data/mysql/ib_logfile0 \
    /qserv/data/mysql/ib_logfile1 \
    /qserv/data/mysql/ibdata1 \
    /qserv/data/mysql/multi-master.info'" ccqserv{101..124}.in2p3.fr

/opt/shmux/bin/shmux -c "echo \"$PASSWORD\" | sudo -S sh -c 'chmod -R o+rx \
    /qserv/data/mysql/q_memoryLockDb'" ccqserv{101..124}.in2p3.fr
