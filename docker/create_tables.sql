CREATE ROLE airflow LOGIN SUPERUSER PASSWORD 'airflow';
CREATE DATABASE airflow
    WITH 
    OWNER = airflow
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;


\connect postgres;

CREATE TABLE IF NOT EXISTS D_Event_type (
  Event_id SERIAL PRIMARY KEY,
  Event_name VARCHAR(55) UNIQUE
);

-- Based on the current unique values from the dataset
INSERT INTO D_Event_type (Event_name) VALUES ('DATA'), ('VOICE'), ('SMS'), ('MMS') 
ON CONFLICT DO NOTHING;


CREATE TABLE IF NOT EXISTS D_scd2_Rate_plan (
  surrogate_uid bigint PRIMARY KEY,
  customer_id bigint,
  Rate_plan_id int,
  Start_D TIMESTAMPTZ,
  End_D TIMESTAMPTZ,
  is_valid boolean
);

CREATE TABLE IF NOT EXISTS datetime_of_insert (
  insert_Date TIMESTAMPTZ PRIMARY KEY,
  batch_id serial UNIQUE
);

CREATE TABLE IF NOT EXISTS F_Usage (
  surrogate_uid bigint,
  Customer_id bigint,
  Start_D TIMESTAMptz,
  Event_id SMALLINT,
  flag1 int,
  flag2 int,
  duration int,
  charge real,
  batch_id smallint,
  CONSTRAINT fk_event_type FOREIGN KEY(surrogate_uid) REFERENCES  D_scd2_Rate_plan(surrogate_uid),
  CONSTRAINT fk_scd2 FOREIGN KEY(Event_id) REFERENCES D_Event_type(Event_id),
  CONSTRAINT batch_id_pk FOREIGN KEY(batch_id) REFERENCES datetime_of_insert(batch_id),
  PRIMARY KEY (Customer_id, Start_D, Event_id)
);