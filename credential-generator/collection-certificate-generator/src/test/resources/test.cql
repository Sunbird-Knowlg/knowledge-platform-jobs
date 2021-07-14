CREATE KEYSPACE sunbird_courses with replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE sunbird_courses.user_enrolments (
    userid text,
    courseid text,
    batchid text,
    active boolean,
    addedby text,
    certificates list<frozen<map<text, text>>>,
    completedon timestamp,
    completionpercentage int,
    contentstatus map<text, int>,
    datetime timestamp,
    enrolleddate text,
    issued_certificates list<frozen<map<text, text>>>,
    lastreadcontentid text,
    lastreadcontentstatus int,
    progress int,
    status int,
    PRIMARY KEY (userid, courseid, batchid)
) WITH CLUSTERING ORDER BY (courseid ASC, batchid ASC);


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
) WITH CLUSTERING ORDER BY (batchid ASC);

//event 1

INSERT INTO sunbird_courses.course_batch(courseid, batchid,cert_templates) VALUES ('do_11309999837886054415','0131000245281587206',{'template_01_dev_001':{'criteria': '{"enrollment":{"status":2}}', 'identifier': 'template_01_dev_001', 'issuer': '{"name":"Gujarat Council of Educational Research and Training","publicKey":["7","8"],"url":"https://gcert.gujarat.gov.in/gcert/"}', 'name': 'Course merit certificate', 'notifyTemplate': '{"subject":"Completion certificate","stateImgUrl":"https://sunbirddev.blob.core.windows.net/orgemailtemplate/img/File-0128212938260643843.png","regardsperson":"Chairperson","regards":"Minister of Gujarat","emailTemplateType":"defaultCertTemp"}', 'signatoryList': '[{"image":"https://cdn.pixabay.com/photo/2014/11/09/08/06/signature-523237__340.jpg","name":"CEO Gujarat","id":"CEO","designation":"CEO"}]'}}) ;

INSERT INTO sunbird_courses.user_enrolments(userid, courseid, batchid,issued_certificates,completedon) VALUES ('user001','do_11309999837886054415','0131000245281587206',[{'identifier': 'certificateId', 'lastIssuedOn': '2019-08-21', 'name': 'Course merit certificate', 'token': 'P4L3Y9'}], toTimeStamp(toDate(now()))) ;

//event 2

INSERT INTO sunbird_courses.course_batch(courseid, batchid,cert_templates) VALUES ('do_11309999837886054416','0131000245281587207',{'template_01_dev_001':{'criteria': '{"enrollment":{"status":2}}', 'identifier': 'template_01_dev_001', 'issuer': '{"name":"Gujarat Council of Educational Research and Training","publicKey":["7","8"],"url":"https://gcert.gujarat.gov.in/gcert/"}', 'name': 'Course merit certificate', 'signatoryList': '[{"image":"https://cdn.pixabay.com/photo/2014/11/09/08/06/signature-523237__340.jpg","name":"CEO Gujarat","id":"CEO","designation":"CEO"}]'}}) ;

INSERT INTO sunbird_courses.user_enrolments(userid, courseid, batchid,completedon) VALUES ('user002','do_11309999837886054416','0131000245281587207', toTimeStamp(toDate(now()))) ;
