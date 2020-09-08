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