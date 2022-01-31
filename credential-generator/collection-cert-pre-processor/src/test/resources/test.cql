CREATE KEYSPACE IF NOT EXISTS sunbird_courses with replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE IF NOT EXISTS sunbird_courses.course_batch (
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

CREATE TABLE IF NOT EXISTS sunbird_courses.user_enrolments (
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
    issued_certificates list<frozen<map<text, text>>>,
    enrolleddate text,
    lastreadcontentid text,
    lastreadcontentstatus int,
    progress int,
    status int,
    PRIMARY KEY (userid, courseid, batchid)
);

CREATE TABLE IF NOT EXISTS sunbird_courses.assessment_aggregator (
    user_id text,
    course_id text,
    batch_id text,
    content_id text,
    attempt_id text,
    created_on timestamp,
    grand_total text,
    last_attempted_on timestamp,
    total_max_score double,
    total_score double,
    updated_on timestamp,
    PRIMARY KEY ((user_id, course_id), batch_id, content_id, attempt_id)
);

CREATE TABLE IF NOT EXISTS sunbird_courses.user_activity_agg (
    activity_id text,
    user_id text,
    activity_type text,
    context_id text,
    agg Map<text, int>,
    aggregates Map<text, double>,
    agg_last_updated Map<text, timestamp>,
    PRIMARY KEY ((activity_type, activity_id), context_id, user_id)
);


//event 1
INSERT INTO sunbird_courses.course_batch(courseid, batchid, cert_templates) VALUES ('do_11309999837886054415','0131000245281587206',{'template_01_dev_001':{'criteria': '{"enrollment":{"status":2}, "assessment": {"score": {">=": 80}}}', 'identifier': 'template_01_dev_001', 'url': 'template-url.svg', 'issuer': '{"name":"Gujarat Council of Educational Research and Training","publicKey":["7","8"],"url":"https://gcert.gujarat.gov.in/gcert/"}', 'name': 'Course Completion Certificate', 'notifyTemplate': '{"subject":"Completion certificate","stateImgUrl":"https://sunbirddev.blob.core.windows.net/orgemailtemplate/img/File-0128212938260643843.png","regardsperson":"Chairperson","regards":"Minister of Gujarat","emailTemplateType":"defaultCertTemp"}', 'signatoryList': '[{"image":"https://cdn.pixabay.com/photo/2014/11/09/08/06/signature-523237__340.jpg","name":"CEO Gujarat","id":"CEO","designation":"CEO"}]'}});
INSERT INTO sunbird_courses.user_enrolments(userid, courseid, batchid, issued_certificates, completedon, status, active) VALUES ('user001','do_11309999837886054415','0131000245281587206',[{'identifier': 'certificateId', 'lastIssuedOn': '2019-08-21', 'name': 'Course Completion Certificate', 'token': 'P4L3Y9'}], toTimeStamp(toDate(now())), 2, true);
INSERT INTO sunbird_courses.assessment_aggregator(user_id, course_id, batch_id, content_id, attempt_id, total_max_score, total_score) VALUES ('user001','do_11309999837886054415','0131000245281587206', 'content_001', 'attempt_001', 1, 1);
INSERT INTO sunbird_courses.user_activity_agg(activity_id, user_id, activity_type, context_id, aggregates) VALUES ('do_11309999837886054415', 'user001', 'Course', 'cb:0131000245281587206', {'score:content_001': 1, 'max_score:content_001': 1});
//event 2
INSERT INTO sunbird_courses.course_batch(courseid, batchid,cert_templates) VALUES ('do_11309999837886054416','0131000245281587207',{'template_01_dev_001':{'criteria': '{"enrollment":{"status":2}}','url': 'template-url.svg', 'identifier': 'template_01_dev_001', 'issuer': '{"name":"Gujarat Council of Educational Research and Training","publicKey":["7","8"],"url":"https://gcert.gujarat.gov.in/gcert/"}', 'name': 'Course merit certificate', 'signatoryList': '[{"image":"https://cdn.pixabay.com/photo/2014/11/09/08/06/signature-523237__340.jpg","name":"CEO Gujarat","id":"CEO","designation":"CEO"}]'}});
INSERT INTO sunbird_courses.user_enrolments(userid, courseid, batchid,completedon) VALUES ('user002','do_11309999837886054416','0131000245281587207', toTimeStamp(toDate(now())));
//empty cert_template
INSERT INTO sunbird_courses.course_batch(courseid, batchid, cert_templates) VALUES ('course_002','batch_002',{});

//event 3
INSERT INTO sunbird_courses.course_batch(courseid, batchid, cert_templates) VALUES ('course_003','batch_003',{'template_01_dev_001':{'criteria': '{"enrollment":{"status":2}, "assessment": {"score": {">=": 80}}}', 'identifier': 'template_01_dev_001', 'url': 'template-url.svg', 'issuer': '{"name":"Gujarat Council of Educational Research and Training","publicKey":["7","8"],"url":"https://gcert.gujarat.gov.in/gcert/"}', 'name': 'Course merit certificate', 'notifyTemplate': '{"subject":"Completion certificate","stateImgUrl":"https://sunbirddev.blob.core.windows.net/orgemailtemplate/img/File-0128212938260643843.png","regardsperson":"Chairperson","regards":"Minister of Gujarat","emailTemplateType":"defaultCertTemp"}', 'signatoryList': '[{"image":"https://cdn.pixabay.com/photo/2014/11/09/08/06/signature-523237__340.jpg","name":"CEO Gujarat","id":"CEO","designation":"CEO"}]'}});
//user with empty issued certificate
INSERT INTO sunbird_courses.user_enrolments(userid, courseid, batchid, issued_certificates, completedon, status, active) VALUES ('user003','course_003','batch_003',[], toTimeStamp(toDate(now())), 2, true);
INSERT INTO sunbird_courses.assessment_aggregator(user_id, course_id, batch_id, content_id, attempt_id, total_max_score, total_score) VALUES ('user003','course_003','batch_003', 'content_001', 'attempt_001', 1, 1);
INSERT INTO sunbird_courses.user_activity_agg(activity_id, user_id, activity_type, context_id, agg) VALUES ('course_003', 'user003', 'Course', 'cb:batch_003', {'score:content_001': 1, 'max_score:content_001': 1});
