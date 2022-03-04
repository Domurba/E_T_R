FROM apache/airflow:2.2.4-python3.9
RUN /usr/local/bin/python -m pip install --upgrade pip
COPY ./requirements.txt . 
RUN pip install -r requirements.txt
USER root

RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME
ENV CLASSPATH=/usr/local/spark/app/postgresql-42.3.3.jar
USER airflow