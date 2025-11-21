# Tarea 3 – Análisis Lingüístico Offline con Hadoop y Pig

Este módulo corresponde a la **Tarea 3** del proyecto de Sistemas Distribuidos.  
Implementa un **análisis batch** sobre datos históricos de Yahoo! Answers y respuestas del LLM, utilizando **Hadoop + HDFS + Apache Pig**.

La Tarea 2 (Kafka, Flink, API, etc.) se mantiene en el repositorio, pero **no es necesario levantarla** para esta entrega. El foco es este módulo de análisis offline.

---

## Estructura del proyecto 

```text
T3 SD/                          # Tambien está el código de la Tarea 2 (streaming, Kafka, Flink, API, etc.)
 ├─ batch/
 │   ├─ Dockerfile              # Imagen de Hadoop + Pig + Python
 │   ├─ scripts/
 │   │   ├─ export_responses.py # Extrae reference_answer y llm_answer desde storage.jsonl
 │   │   ├─ load_to_hdfs.sh     # Carga archivos a HDFS (/input/...)
 │   │   └─ run_pig_jobs.sh     # Ejecuta scripts Pig de wordcount (human vs LLM)
 │   ├─ pig/
 │   │   ├─ wordcount.pig       # Lógica de limpieza, tokenización, stopwords y conteo
 │   │   └─ stopwords.txt       # Lista de palabras vacías
 │   └─ data/                   # Archivos de texto y resultados
 ├─ storage.jsonl               # Dump histórico con reference_answer y llm_answer
 ├─ docker-compose.yml          # Define servicio hadoop-pig (y db de la T2 si se requiere)
 └─ analysis_wordfreq.py        # Script local para top 50 y gráficos
```



### 1. Construir la imagen del módulo batch

Desde la raíz del repositorio:

```bash
docker compose build hadoop-pig
```

### 2. Levantar el contenedor de Hadoop + Pig

```bash
docker compose up -d hadoop-pig
docker compose ps
```

### 3. Inicialización dentro del contenedor

Entrar al contenedor:

```bash
docker compose exec hadoop-pig bash
cd /opt/batch
```

#### 3.1 (Primera vez) Configurar SSH sin contraseña

```bash
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
ssh-keyscan -H localhost >> ~/.ssh/known_hosts
ssh localhost hostname   # Debe funcionar sin pedir password
```

#### 3.2 Configurar `JAVA_HOME` para Hadoop

```bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
echo "export JAVA_HOME=$JAVA_HOME" >> /opt/hadoop/etc/hadoop/hadoop-env.sh
grep JAVA_HOME /opt/hadoop/etc/hadoop/hadoop-env.sh
```

### 4. Levantar HDFS

Dentro del contenedor:

```bash
service ssh start
/opt/hadoop/bin/hdfs namenode -format -force    # sólo la primera vez
/opt/hadoop/sbin/start-dfs.sh
jps   # Debe mostrar NameNode, DataNode, SecondaryNameNode
```

### 5. Exportar datos desde `storage.jsonl`

Desde `/opt/batch`:

```bash
python3 scripts/export_responses.py
ls data
# Deberías ver: human_responses.txt y llm_responses.txt
```

### 6. Cargar datos a HDFS

```bash
/opt/batch/scripts/load_to_hdfs.sh
hdfs dfs -ls /input
hdfs dfs -ls /input/human
hdfs dfs -ls /input/llm
```

### 7. Subir stopwords a HDFS

```bash
hdfs dfs -mkdir -p /input/stopwords
hdfs dfs -put -f /opt/batch/pig/stopwords.txt /input/stopwords/stopwords.txt
```

### 8. Ejecutar scripts de Pig (wordcount)

```bash
/opt/batch/scripts/run_pig_jobs.sh
hdfs dfs -ls /output
hdfs dfs -ls /output/human_wordcount
hdfs dfs -ls /output/llm_wordcount
```

Ver algunos resultados:

```bash
hdfs dfs -cat /output/human_wordcount/part-r-00000 | head
hdfs dfs -cat /output/llm_wordcount/part-r-00000 | head
```

### 9. Descargar resultados para análisis local

Dentro del contenedor:

```bash
hdfs dfs -get /output /opt/batch/data/output
ls /opt/batch/data/output
```

Luego salir del contenedor:

```bash
exit
```

En el host:

```bash
ls batch/data/output
```

### 10. Generar top 50 y gráficos con Python (host)

Desde la raíz del repo:

```bash
python3 analysis_wordfreq.py
```

Esto genera:

- `batch/data/analysis/human_top50.csv`
- `batch/data/analysis/llm_top50.csv`
- `batch/data/analysis/human_top50.png`
- `batch/data/analysis/llm_top50.png`



