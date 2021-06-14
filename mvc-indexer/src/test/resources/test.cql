CREATE KEYSPACE IF NOT EXISTS local_content_store WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS local_content_store.content_data (
  content_id text,
  last_updated_on timestamp,
  body blob,
  oldBody blob,
  stageIcons blob,
  screenshots blob,
  externallink text,
  PRIMARY KEY (content_id)
);