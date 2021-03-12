CREATE KEYSPACE IF NOT EXISTS platform_db WITH replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE IF NOT EXISTS platform_db.job_request (
    client_key text,
    request_id text,
    dt_expiration timestamp,
    dt_file_created timestamp,
    dt_first_event timestamp,
    dt_job_completed timestamp,
    dt_job_processing timestamp,
    dt_job_submitted timestamp,
    dt_last_event timestamp,
    err_message text,
    execution_time bigint,
    file_size bigint,
    input_events int,
    iteration int,
    job_id text,
    latency int,
    location text,
    output_events int,
    request_data text,
    stage text,
    stage_status text,
    status text,
    PRIMARY KEY (client_key, request_id)
);
