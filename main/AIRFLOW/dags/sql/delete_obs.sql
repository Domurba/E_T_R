DELETE FROM f_usage where batch_id in
(select batch_id  from datetime_of_insert
where now() - insert_date > interval '6 months')