# Telco

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project </a>
      <ul> 
      <li><a href="#anwsers-to-questions">Anwsers to questions</a></li>
      </ul> 
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
      </ul>      
    </li>
    <li><a href="#Runing-the-project">Runing the project</a></li>
  </ol>
</details>


<!-- ABOUT THE Project -->
## About The Project
**Deocker** creates containers with Postgres, PgAdmin, Airflow and Spark.

The **DWH** uses a snowflake schema. The Schema is predefined in the docker image.
In addition, the table used for storage of user id's and their rate plans uses a SCD2 (Slowly changing dimensions Type2) table in order to track history and changes to the customers plans.

**SPARK** reads the csv file, checks for duplicate combinations of (Customer_id, Start_time and event_type). It saves the duplicate values into a new csv file.
Afterwards, Spark is used to apply transformations on the data. The transformed data is loaded into Postgres with the help of a JDBC connector.  

**Airflow** starts the ETL process. It sends and email if a csv file with duplicate primary keys exists, after which the file is deleted. It also checks if any of the records are inserted after the 6 month period using the batch_id data and Postgres Operator.

### Anwsers to questions
3. **Explain how you would implement support and monitoring process.**

PgAdmin dashboards can be an easy way to monitor the load on the database. 
Airflow can be set up to email on retry or on failiure. In addition, airflow provides logs, so tracking of issues is possible.

4. How do you see the evolution of the DWH project in "Telco Relax"? What will be major organisational and technical challenges?

Adding .env files and reading them with the "dotenv" package would be better.
The project currently uses and is configured to run on one machine, so this is it's bottleneck.
Tests should be written in order to see effects of additional changes.

The only database that it uses is a relational database, meaning that data from other sources will be more difficult to add, as the schema needs to be changed. On the other hand, NoSQL databases dont have a schema, which makes it easier to add new data to existing objects. 

Additional data quality checks may be required. For example, checking if the date of the events in a batch are current and if these values do not overlap with previous data.
No null values found in the current batch, but future batch checks might be required

With larger ammounts of data, the single node can run out of starage space, experience memory issues and run slower due to context switching. 
In addition, Airflow uses Local Executor mode, which slows down the process, because the number of workers is limited to one. In production, Celery executor would be better.

We can scale and configure spark to run on more worker nodes by adding more workers and connecting them to machines.
These processes would require technical expertise and optimisation. On one hand, these docker images can be configures to run on managed cloud infrastructure, such as EKS or ECS. Another way woould be to scale the infrastructure out on premmise.

Additionaly, we could use tools such as SparkStreaming to stram the data in real time and not do batch processing each week.

Data analyst team would need to choose a tool, such as Tableau, for visualisations. Before hand, data can be grouped and stored in an OLAP database, such as a materialized view, in order to cut down on the ammount of processing.

DB ERD

![ERD_Diagram](https://user-images.githubusercontent.com/91464837/156760046-9b60f832-020a-4559-ba1b-8984d098e16c.png)

Airflow DAG

![Airflow_DAG](https://user-images.githubusercontent.com/91464837/156760098-bea027a5-3d36-49f6-a297-0e0f7b75e702.png)

Airflow DAG runs

![DAG_runs](https://user-images.githubusercontent.com/91464837/156760138-113a0846-e65e-44ea-b76a-837af353697c.png)

SCD2 and f_usage tables

![SCD2_table](https://user-images.githubusercontent.com/91464837/156760178-bf278116-04d9-4ffa-bca4-39734e500a9f.png)
![f_usage_table](https://user-images.githubusercontent.com/91464837/156760181-0fd1e09c-c6fa-4e83-bb7e-81582ec1e114.png)

### Prerequisites
[Docker](https://docs.docker.com/get-docker/)  
[Docker-compose](https://docs.docker.com/compose/install/)  


# Running the project
1. In parent directory, build the project infrastructure (Postgres, pgAdmin, Spark, Airflow)
  ```sh
  docker-compose -f ./docker/Docker-compose.yaml build
  docker-compose -f ./docker/Docker-compose.yaml up
  ```

2. File location
  ```sh
  The file should be located in the ./main/SPARK/resources folder. That way, spark workers and airflow scheduler can see the file, because it is mounted to those containers.
  ```
3. Adding Spark and Postgres connections in Airflow
  ```sh
  Note, that this is the defaul name of the webserver container
  
  docker exec -it docker-airflow-webserver-1 sh

  airflow connections add 'spark_conn' \
  --conn-type 'spark' \
  --conn-host 'spark://spark' \
  --conn-port '7077'

  airflow connections add 'postgres_sql' \
  --conn-type 'postgres' \
  --conn-host 'postgres' \
  --conn-schema 'postgres' \
  --conn-login 'postgres' \
  --conn-port '5432'  

4. Go to this address for the Airflow UI. Admin > Connections and click on edit postgres connection. Add password (Default is postgres)
  ```sh
  http://localhost:8080/
  ```

5. When done, stop project infrastructure
  ```sh
  docker-compose -f ./docker/Docker-compose.yaml down
  ```

- Remove volumes (Optional)
  ```sh 
  docker volume prune
  ```

- Shutdown Windows Subsystem for Linux (optional)
  ```sh 
  wsl --shutdown
  ```
## Docker services info
###### Postgres Server info
- Default host name (Docker container name): **postgres**
- Default port: **5432**
- Default server name (value of the "POSTGRES_USER" variable): **postgres**
- Default username: **postgres**
- Defaul password: **postgres**

###### pgAdmin
  ```sh
  http://localhost:5050/
  ```
- Default email: **example@email.com**  
- Default password: **admin**  

###### AIRFLOW 
  ```sh
  http://localhost:8080/
  ```
- Default username: **airflow**
- Default password: **airflow**
