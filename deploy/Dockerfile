FROM eclipse-temurin:8-jdk-focal

ARG HADOOP_VERSION=3.3.6

ARG HADOOP_USER=epfuser

RUN apt-get update && \
    apt-get install -y ssh rsync sudo && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash $HADOOP_USER && \
    echo "$HADOOP_USER:$HADOOP_USER" | chpasswd && \
    mkdir -p /home/$HADOOP_USER/.ssh && \
    ssh-keygen -t rsa -P "" -f /home/$HADOOP_USER/.ssh/id_rsa && \
    cat /home/$HADOOP_USER/.ssh/id_rsa.pub >> /home/$HADOOP_USER/.ssh/authorized_keys && \
    chown -R $HADOOP_USER:$HADOOP_USER /home/$HADOOP_USER/.ssh && \
    chmod 600 /home/$HADOOP_USER/.ssh/authorized_keys

RUN echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

RUN curl -L --retry 3 https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz | \
    tar -xz -C /opt && \
    ln -s /opt/hadoop-$HADOOP_VERSION /opt/hadoop && \
    chown -R $HADOOP_USER:$HADOOP_USER /opt/hadoop-$HADOOP_VERSION

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

COPY --chown=$HADOOP_USER:$HADOOP_USER core-site.xml $HADOOP_CONF_DIR/core-site.xml
COPY --chown=$HADOOP_USER:$HADOOP_USER hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml

COPY core-site.xml $HADOOP_HOME/etc/hadoop/
COPY hdfs-site.xml $HADOOP_HOME/etc/hadoop/
COPY mapred-site.xml $HADOOP_HOME/etc/hadoop/
COPY yarn-site.xml $HADOOP_HOME/etc/hadoop/

RUN echo "export JAVA_HOME=/opt/java/openjdk/" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
RUN echo "export JAVA_HOME=/opt/java/openjdk" >> /home/$HADOOP_USER/.bashrc
RUN echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> /home/$HADOOP_USER/.bashrc

RUN echo "export HADOOP_HOME=/opt/hadoop" >> /home/$HADOOP_USER/.bashrc
RUN echo "export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop" >> /home/$HADOOP_USER/.bashrc
RUN echo "export PATH=\$PATH:/opt/hadoop/bin:/opt/hadoop/sbin" >> /home/$HADOOP_USER/.bashrc

EXPOSE 9870 9000 8088 9864

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]