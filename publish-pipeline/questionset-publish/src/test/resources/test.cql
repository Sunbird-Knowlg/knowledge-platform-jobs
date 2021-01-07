CREATE KEYSPACE dev_hierarchy_store with replication = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS dev_hierarchy_store.questionset_hierarchy (
  identifier text,
  hierarchy text,
  PRIMARY KEY (identifier)
);