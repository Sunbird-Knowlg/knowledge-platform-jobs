CREATE KEYSPACE IF NOT EXISTS local_platform_db WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS local_platform_db.job_request (
    client_key text,
    request_id text,
    job_id text,
    status text,
    request_data text,
    location text,
    dt_file_created timestamp,
    dt_first_event timestamp,
    dt_last_event timestamp,
    dt_expiration timestamp,
    iteration int,
    dt_job_submitted timestamp,
    dt_job_processing timestamp,
    dt_job_completed timestamp,
    input_events int,
    output_events int,
    file_size bigint,
    latency int,
    execution_time bigint,
    err_message text,
    stage text,
    stage_status text,
    job_name text,
    PRIMARY KEY (client_key, request_id)
);