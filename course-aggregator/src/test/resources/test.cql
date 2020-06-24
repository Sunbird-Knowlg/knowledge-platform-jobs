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


/*INSERT INTO sunbird_courses.assessment_aggregator (course_id,batch_id,user_id,content_id,attempt_id,created_on,grand_total
,last_attempted_on,question,total_max_score,total_score,updated_on) VALUES
('do_11287774806987571218','0128777534861066242','8454cb21-3ce9-4e30-85b5-fade097880d8','do_112876961957437440179',
'0afe2cc175ffc95b83a4dc8d1af2c47b','2019-11-07 05:38:49.640+0000','2/5','2019-11-07 05:38:05.076+0000',null,
5,2,'2019-11-07 05:38:49.642+0000');*/

