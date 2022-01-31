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
INSERT INTO sunbird_courses.user_content_consumption(userid, contentid, batchid,courseid,progress,status,viewcount,completedcount) VALUES ('8454cb21-3ce9-4e30-85b5-fade097880d8','do_11260735471149056012299','0126083288437637121','do_1127212344324751361295',100, 2, 3,1) ;
INSERT INTO sunbird_courses.user_content_consumption(userid, contentid, batchid,courseid,progress,status,viewcount,completedcount) VALUES ('8454cb21-3ce9-4e30-85b5-fade097880d8','do_11260735471149056012300','0126083288437637121','do_1127212344324751361295',100, 2, 2,2) ;
INSERT INTO sunbird_courses.user_content_consumption(userid, contentid, batchid,courseid,progress,status,viewcount,completedcount) VALUES ('8454cb21-3ce9-4e30-85b5-fade097880d8','do_11260735471149056012301','0126083288437637121','do_1127212344324751361295',0, 1,0,1) ;

//Event_2 Testcase Data
INSERT INTO sunbird_courses.user_content_consumption(userid, contentid, batchid,courseid,progress,status,viewcount,completedcount) VALUES ('user001','do_R1','Batch1','course001',100, 2,1,0) ;
INSERT INTO sunbird_courses.user_content_consumption(userid, contentid, batchid,courseid,progress,status,viewcount,completedcount) VALUES ('user001','do_R2','Batch1','course001',100, 2,1,0) ;
INSERT INTO sunbird_courses.user_content_consumption(userid, contentid, batchid,courseid,progress,status,viewcount,completedcount) VALUES ('user001','do_R3','Batch1','course001',100, 2,1,0) ;



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
) WITH CLUSTERING ORDER BY (courseid ASC, batchid ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
CREATE INDEX inx_ues_status ON sunbird_courses.user_enrolments (status);
CREATE INDEX inx_ues_certs ON sunbird_courses.user_enrolments (values(certificates));


INSERT INTO sunbird_courses.user_enrolments(userid, courseid, batchid, status) VALUES ('user001', 'course001', 'Batch1', 1)
