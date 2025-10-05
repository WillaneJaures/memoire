FROM astrocrpublic.azurecr.io/runtime:3.0-4

#FROM quay.io/astronomer/astro-runtime:9.3.0  # ou autre version compatible

# Installe Java et Spark
USER root

# Installer Java et Spark
RUN apt-get update && apt-get install -y openjdk-17-jdk wget curl unzip && \
    wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xvzf spark-3.5.1-bin-hadoop3.tgz -C /opt/ && \
    ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm spark-3.5.1-bin-hadoop3.tgz

# Configurer JAVA_HOME et SPARK_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# 📌 Copier les DAGs et scripts Spark
COPY dags/ /usr/local/airflow/dags/
COPY spark_jobs/ /spark_jobs/

#copier le driver mysql dans le conteneur
COPY drivers/mysql-connector-java-9.3.0.jar /opt/spark/jars/


USER astro