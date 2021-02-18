CREATE KEYSPACE sunbird_courses with replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE sunbird_courses.course_batch (
    courseid text,
    batchid text,
    cert_templates map<text, frozen<map<text, text>>>,
    createdby text,
    createddate text,
    createdfor list<text>,
    description text,
    enddate text,
    enrollmentenddate text,
    enrollmenttype text,
    mentors list<text>,
    name text,
    startdate text,
    status int,
    updateddate text,
    PRIMARY KEY (courseid, batchid)
);

CREATE KEYSPACE dialcodes with replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE dialcodes.dialcode_images (
    filename text PRIMARY KEY,
    channel text,
    config map<text, text>,
    created_on timestamp,
    dialcode text,
    publisher text,
    status int,
    url text
);

INSERT INTO dialcodes.dialcode_images(filename, channel, dialcode, url) VALUES ('0_Q1I5I3', 'b00bc992ef25f1a9a8d63291e20efc8d', 'Q1I5I3', 'https://sunbirddev.blob.core.windows.net/sunbird-content-dev/in.ekstep/0_Q1I5I3.png') ;
