#!/bin/bash

mkdir -p /run/sshd

ssh-keygen -A

/usr/sbin/sshd

su - $HADOOP_USER -c "
    export HDFS_NAMENODE_USER=epfuser
    export HDFS_DATANODE_USER=epfuser
    export HDFS_SECONDARYNAMENODE_USER=epfuser
    export YARN_RESOURCEMANAGER_USER=epfuser
    export YARN_NODEMANAGER_USER=epfuser

    if [ ! -d \"/home/$HADOOP_USER/hdfs/namenode/current\" ]; then
        $HADOOP_HOME/bin/hdfs namenode -format -force
    fi

    $HADOOP_HOME/sbin/start-dfs.sh

    $HADOOP_HOME/bin/hdfs dfsadmin -safemode leave
    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /data

    $HADOOP_HOME/sbin/start-yarn.sh

    tail -f /dev/null
"