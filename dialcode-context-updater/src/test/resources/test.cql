CREATE KEYSPACE IF NOT EXISTS dialcode_store with replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE IF NOT EXISTS dialcode_store.dial_code (identifier text,dialcode_index double,publisher text,channel text,batchCode text,metadata text,status text,generated_on text,published_on text, primary key(identifier));

INSERT INTO dialcode_store.dial_code(identifier, dialcode_index, publisher, batchCode, status, generated_on) VALUES ('0117CH01', 0.0, '0125196274181898243', '0125196274181898243-01', 'Draft', '2020-03-04T14:15:00.000');
