FROM eclipse-temurin:8-jdk

# Variables de entorno básicas
ENV HADOOP_VERSION=2.10.2
ENV PIG_VERSION=0.17.0
ENV HADOOP_HOME=/opt/hadoop
ENV PIG_HOME=/opt/pig
ENV PATH=$PATH:$HADOOP_HOME/bin:$PIG_HOME/bin

# Instalar dependencias
RUN apt-get update && \
    apt-get install -y wget python3 python3-pip ssh rsync vim && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt

# Descargar e instalar Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Configuración mínima de Hadoop pseudo-distribuido
RUN mkdir -p /opt/hadoop/hdfs/namenode && \
    mkdir -p /opt/hadoop/hdfs/datanode

# Configuración core-site.xml
RUN cat <<"EOF" > /opt/hadoop/etc/hadoop/core-site.xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF

# Configuración hdfs-site.xml
RUN cat <<"EOF" > /opt/hadoop/etc/hadoop/hdfs-site.xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:/opt/hadoop/hdfs/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:/opt/hadoop/hdfs/datanode</value>
  </property>
</configuration>
EOF

# Descargar e instalar Pig
RUN wget https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz && \
    tar -xzf pig-${PIG_VERSION}.tar.gz && \
    mv pig-${PIG_VERSION} pig && \
    rm pig-${PIG_VERSION}.tar.gz

WORKDIR /opt/batch

# Copiar scripts y archivos de Pig al contenedor
COPY scripts ./scripts
COPY pig ./pig
COPY data ./data

# Script para iniciar HDFS y mantener el contenedor vivo
RUN cat <<"EOF" > /opt/start.sh
#!/bin/bash
set -e

# Iniciar SSH requerido por Hadoop (ignorar errores)
service ssh start || true

# Formatear HDFS solo la primera vez
if [ ! -d "/opt/hadoop/hdfs/namenode/current" ]; then
    echo "Formateando HDFS..."
    /opt/hadoop/bin/hdfs namenode -format -force
fi

# Iniciar NameNode y DataNode
/opt/hadoop/sbin/start-dfs.sh

echo "HDFS iniciado. Dejando el contenedor en bash."

# Mantener el contenedor en primer plano para que Docker lo considere en ejecución
tail -f /dev/null
EOF
RUN cat <<"EOF" > /opt/start.sh
#!/bin/bash
set -e

# Iniciar SSH requerido por Hadoop (ignorar errores)
service ssh start || true

# Formatear HDFS solo la primera vez
if [ ! -d "/opt/hadoop/hdfs/namenode/current" ]; then
  echo "Formateando HDFS..."
  /opt/hadoop/bin/hdfs namenode -format -force
fi

# Iniciar NameNode y DataNode
/opt/hadoop/sbin/start-dfs.sh

echo "HDFS iniciado. Manteniendo el contenedor en primer plano."
# Mantener el contenedor en primer plano para que Docker lo considere en ejecución
tail -f /dev/null
EOF

RUN chmod +x /opt/start.sh

CMD ["/opt/start.sh"]

