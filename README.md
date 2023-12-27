## Packages
```
// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
pyspark --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0

// https://mvnrepository.com/artifact/com.mysql/mysql-connector-j
implementation 'com.mysql:mysql-connector-j:8.2.0'
```

## Datalake - Cassandra
### Table schema
```
.
root
 |-- create_time: string (nullable = false)
 |-- bid: integer (nullable = true)
 |-- bn: string (nullable = true)
 |-- campaign_id: integer (nullable = true)
 |-- cd: integer (nullable = true)
 |-- custom_track: string (nullable = true)
 |-- de: string (nullable = true)
 |-- dl: string (nullable = true)
 |-- dt: string (nullable = true)
 |-- ed: string (nullable = true)
 |-- ev: integer (nullable = true)
 |-- group_id: integer (nullable = true)
 |-- id: string (nullable = true)
 |-- job_id: integer (nullable = true)
 |-- md: string (nullable = true)
 |-- publisher_id: integer (nullable = true)
 |-- rl: string (nullable = true)
 |-- sr: string (nullable = true)
 |-- ts: string (nullable = true)
 |-- tz: integer (nullable = true)
 |-- ua: string (nullable = true)
 |-- uid: string (nullable = true)
 |-- utm_campaign: string (nullable = true)
 |-- utm_content: string (nullable = true)
 |-- utm_medium: string (nullable = true)
 |-- utm_source: string (nullable = true)
 |-- utm_term: string (nullable = true)
 |-- v: integer (nullable = true)
 |-- vp: string (nullable = true)
```


## Datawarehouse - MySQL
- job
- company
- events

### Table schema
```
 job
 |-- id: string (nullable = true)
 |-- created_by: string (nullable = true)
 |-- created_date: string (nullable = true)
 |-- last_modified_by: string (nullable = true)
 |-- last_modified_date: string (nullable = true)
 |-- is_active: string (nullable = true)
 |-- title: string (nullable = true)
 |-- description: string (nullable = true)
 |-- work_schedule: string (nullable = true)
 |-- radius_unit: string (nullable = true)
 |-- location_state: string (nullable = true)
 |-- location_list: string (nullable = true)
 |-- role_location: string (nullable = true)
 |-- resume_option: string (nullable = true)
 |-- budget: string (nullable = true)
 |-- status: string (nullable = true)
 |-- error: string (nullable = true)
 |-- template_question_id: string (nullable = true)
 |-- template_question_name: string (nullable = true)
 |-- question_form_description: string (nullable = true)
 |-- redirect_url: string (nullable = true)
 |-- start_date: string (nullable = true)
 |-- end_date: string (nullable = true)
 |-- close_date: string (nullable = true)
 |-- group_id: string (nullable = true)
 |-- minor_id: string (nullable = true)
 |-- campaign_id: string (nullable = true)
 |-- company_id: string (nullable = true)
 |-- history_status: string (nullable = true)
 |-- ref_id: string (nullable = true)

 company
 |-- id: string (nullable = true)
 |-- created_by: string (nullable = true)
 |-- created_date: string (nullable = true)
 |-- last_modified_by: string (nullable = true)
 |-- last_modified_date: string (nullable = true)
 |-- is_active: string (nullable = true)
 |-- name: string (nullable = true)
 |-- is_agree_conditon: string (nullable = true)
 |-- is_agree_sign_deal: string (nullable = true)
 |-- sign_deal_user: string (nullable = true)
 |-- billing_id: string (nullable = true)
 |-- manage_type: string (nullable = true)
 |-- customer_type: string (nullable = true)
 |-- status: string (nullable = true)
 |-- publisher_id: string (nullable = true)
 |-- flat_rate: string (nullable = true)
 |-- percentage_of_click: string (nullable = true)
 |-- logo: string (nullable = true)

 events
 |-- id: string (nullable = true)
 |-- job_id: string (nullable = true)
 |-- dates: string (nullable = true)
 |-- hours: string (nullable = true)
 |-- disqualified_application: string (nullable = true)
 |-- qualified_application: string (nullable = true)
 |-- conversion: string (nullable = true)
 |-- company_id: string (nullable = true)
 |-- group_id: string (nullable = true)
 |-- campaign_id: string (nullable = true)
 |-- publisher_id: string (nullable = true)
 |-- bid_set: string (nullable = true)
 |-- clicks: string (nullable = true)
 |-- impressions: string (nullable = true)
 |-- spend_hour: string (nullable = true)
 |-- sources: string (nullable = true)
 ```
