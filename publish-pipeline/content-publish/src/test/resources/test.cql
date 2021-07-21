CREATE KEYSPACE IF NOT EXISTS dev_content_store with replication = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS dev_content_store.content_data (
  identifier text,
  body blob,
  editorstate text,
  answer blob,
  solutions text,
  instructions text,
  hints text,
  media text,
  responsedeclaration text,
  interactions text,
  PRIMARY KEY (identifier)
);
