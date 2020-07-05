CREATE KEYSPACE sunbird_courses with replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE sunbird_courses.user_content_consumption (
    userid text,
    courseid text,
    batchid text,
    contentid text,
    completedcount int,
    datetime timestamp,
    lastaccesstime text,
    lastcompletedtime text,
    lastupdatedtime text,
    progress int,
    status int,
    viewcount int,
    PRIMARY KEY (userid, courseid, batchid, contentid)
) WITH CLUSTERING ORDER BY (courseid ASC, batchid ASC, contentid ASC);

// EVENT_1 Testcase data
INSERT INTO sunbird_courses.user_content_consumption(userid, contentid, batchid,courseid,progress,status) VALUES ('8454cb21-3ce9-4e30-85b5-fade097880d8','do_11260735471149056012299','0126083288437637121','do_1127212344324751361295',100, 2) ;
INSERT INTO sunbird_courses.user_content_consumption(userid, contentid, batchid,courseid,progress,status) VALUES ('8454cb21-3ce9-4e30-85b5-fade097880d8','do_11260735471149056012300','0126083288437637121','do_1127212344324751361295',100, 2) ;
INSERT INTO sunbird_courses.user_content_consumption(userid, contentid, batchid,courseid,progress,status) VALUES ('8454cb21-3ce9-4e30-85b5-fade097880d8','do_11260735471149056012301','0126083288437637121','do_1127212344324751361295',0, 1) ;

//Event_2 Testcase Data
INSERT INTO sunbird_courses.user_content_consumption(userid, contentid, batchid,courseid,progress,status) VALUES ('user001','do_R1','Batch1','course001',100, 2) ;
INSERT INTO sunbird_courses.user_content_consumption(userid, contentid, batchid,courseid,progress,status) VALUES ('user001','do_R2','Batch1','course001',100, 2) ;
INSERT INTO sunbird_courses.user_content_consumption(userid, contentid, batchid,courseid,progress,status) VALUES ('user001','do_R3','Batch1','course001',100, 2) ;



CREATE TABLE IF NOT EXISTS sunbird_courses.activity_user_agg (
    activity_id text,
    user_id text,
    activity_type text,
    context_id text,
    agg Map<text, int>,
    agg_last_updated Map<text, timestamp>,
    PRIMARY KEY (activity_type, activity_id, context_id, user_id)
);
