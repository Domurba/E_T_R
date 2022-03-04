from datetime import timedelta, datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/resources/postgresql-42.3.3.jar"

csv_file = "/usr/local/spark/resources/sample.csv"
postgres_db = "jdbc:postgresql://postgres/postgres"
postgres_user = "postgres"
postgres_pwd = "postgres"

html_email = """<h3>Duplicate values detected in latest batch
            {}""".format(
    datetime.now()
)


def branch_email():
    if Path("/usr/local/spark/resources/duplicates.csv").is_file():
        return "email"
    return "dummy"


default_args = {
    "owner": "D.U.",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(hours=1),
    "email": ["du_airflow@protonmail.com"],
    "email_on_failiure": True,
}
with DAG(
    "Spark_PG_tasks",
    description="spark_actions_on_database",
    catchup=False,
    schedule_interval="0 0 * * 0",  # Weekly
    default_args=default_args,
) as dag:

    start = DummyOperator(task_id="start")
    dummy = DummyOperator(task_id="dummy")
    end = DummyOperator(task_id="end", trigger_rule="none_failed")

    check = SparkSubmitOperator(
        task_id="spark_check",
        application="/usr/local/spark/app/spark_check_for_quality.py",
        conn_id="spark_conn",
        name="spark_check",
        verbose=1,
        conf={"spark.master": spark_master},
        application_args=[
            csv_file,
            postgres_db,
            postgres_user,
            postgres_pwd,
        ],
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
    )

    brancher = BranchPythonOperator(
        task_id="Branch_if_duplicates",
        python_callable=branch_email,
    )

    emailer = EmailOperator(
        task_id="email",
        to="du_airflow@protonmail.com",
        subject="duplicate and unclean data",
        html_content=html_email,
        files=["/usr/local/spark/resources/duplicates.csv"],
    )
    deleter = BashOperator(
        task_id="del_dups",
        bash_command="rm /usr/local/spark/resources/duplicates.csv",
    )

    write = SparkSubmitOperator(
        task_id="spark_write",
        application="/usr/local/spark/app/spark_write.py",
        conn_id="spark_conn",
        name="sparky1",
        verbose=1,
        conf={"spark.master": spark_master},
        application_args=[
            csv_file,
            postgres_db,
            postgres_user,
            postgres_pwd,
        ],
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
        trigger_rule="one_success",
    )

    check_for_deletion = PostgresOperator(
        task_id="check_for_deletion",
        postgres_conn_id="postgres_sql",
        sql="sql/delete_obs.sql",
    )

    start >> check >> brancher
    brancher >> emailer >> deleter >> write >> check_for_deletion >> end
    brancher >> dummy >> write >> check_for_deletion >> end
