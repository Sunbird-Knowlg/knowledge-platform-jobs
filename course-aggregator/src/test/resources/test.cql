CREATE KEYSPACE sunbird_courses with replication = {'class':'SimpleStrategy','replication_factor':1};


CREATE TABLE IF NOT EXISTS sunbird_courses.activity_aggregator (
activity_type text,
activity_id text,
user_id text,
context_id text,
agg Map<text, double>,
agg_last_updated Map<text, timestamp>,
PRIMARY KEY (activity_type, activity_id, user_id)
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
    delta text,
    enrolleddate text,
    grade text,
    lastreadcontentid text,
    lastreadcontentstatus int,
    progress int,
    status int,
    PRIMARY KEY (userid, courseid, batchid)
);

//INSERT INTO sunbird_courses.user_enrolments (userid,courseid,batchid,active,addedby,completedon,completionpercentage ,contentstatus,delta,lastreadcontentid, lastreadcontentstatus, progress, status) VALUES ('user01','course01','batch01', true,'manju','2020-11-07 05:38:49.640+0000',80,{'do_124':2}, 'delta', 'do_11260735471149056012300', 2, 20, 1);
//INSERT INTO sunbird_courses.user_enrolments (userid,courseid,batchid,active,addedby,completedon,completionpercentage ,contentstatus,delta,lastreadcontentid, lastreadcontentstatus, progress, status) VALUES ('8454cb21-3ce9-4e30-85b5-fade097880d8','do_1127212344324751361295','0126083288437637121', true,'manju','2020-11-07 05:38:49.640+0000',80,{'do_11260735471149056012300':2}, 'delta', 'do_124', 2, 20, 1);
