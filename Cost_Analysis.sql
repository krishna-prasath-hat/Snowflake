select warehouse_name, warehouse_id, sum(credits_used) as credits from snowflake.account_usage.warehouse_metering_history where start_time >= TO_TIMESTAMP_LTZ('2024-07-17T00:00:00Z', 'auto') and start_time < TO_TIMESTAMP_LTZ('2024-07-19T00:00:00Z', 'auto') group by 1, 2 order by 3 desc limit 1000;


select * from snowflake.account_usage.warehouse_metering_history;


SELECT query_id
     ,warehouse_name
     ,start_time
     ,end_time
     ,total_elapsed_sec
     ,case
            when total_elapsed_sec < 60 then 60 
            else total_elapsed_sec
      end as total_elapsed_sec_1
     ,ROUND(unit_of_credit*total_elapsed_sec_1 / 60/60,2)  total_credit
     ,total_credit*3.00 query_cost --change based on how much you are paying for a credit
FROM (
  select query_id
     ,warehouse_name
     ,start_time
     ,end_time
     ,total_elapsed_time/1000   total_elapsed_sec
     ,CASE WHEN warehouse_size = 'X-Small'    THEN 1
             WHEN warehouse_size = 'Small'      THEN 2
             WHEN warehouse_size = 'Medium'     THEN 4
             WHEN warehouse_size = 'Large'      THEN 8
             WHEN warehouse_size = 'X-Large'    THEN 16
             WHEN warehouse_size = '2X-Large'   THEN 32
             WHEN warehouse_size = '3X-Large'   THEN 64
             WHEN warehouse_size = '4X-Large'   THEN 128
       ELSE 1    
       END unit_of_credit
       from table(information_schema.QUERY_HISTORY_BY_USER
             (user_name => 'Jerish', 
              END_TIME_RANGE_START => dateadd('hours',-1,current_timestamp()), 
              END_TIME_RANGE_END => current_timestamp(),RESULT_LIMIT => 10000)))
              where QUERY_ID = '685afd62b10af1e5e01db7be2041f0c7';


//query to calculate cost of a query
select * 
              from table(information_schema.QUERY_HISTORY_BY_USER
             (user_name => 'Jerish', 
              END_TIME_RANGE_START => dateadd('days',-5,current_timestamp()), 
              END_TIME_RANGE_END => current_timestamp(),RESULT_LIMIT => 10000))
              where QUERY_ID = '685afd62b10af1e5e01db7be2041f0c7';
;