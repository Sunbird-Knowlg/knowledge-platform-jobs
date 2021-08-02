CREATE KEYSPACE IF NOT EXISTS dev_hierarchy_store with replication = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS dev_hierarchy_store.questionset_hierarchy (
  identifier text,
  hierarchy text,
  instructions text,
  PRIMARY KEY (identifier)
);

CREATE KEYSPACE IF NOT EXISTS dev_question_store with replication = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS dev_question_store.question_data (
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


INSERT INTO dev_question_store.question_data(identifier, body, editorstate, answer, solutions, media, responsedeclaration) VALUES (
'do_113188615625731',
textAsBlob('<div class="question-body"><div class="mcq-title"><p>Color of Sky is?</p></div><div data-choice-interaction="response1" class="mcq-vertical"></div></div>'),
'{"question":"<p>Capital of india is?</p>","answer":"<p>New Delhi</p>"}',
textAsBlob('1'),
'[{"id": "c012a8a9-f78b-6ddb-3ac2-bd1f38c7850b", "type": "html", "value": "<p>Color of sky is blue&nbsp;</p><figure class=\"image\"><img src=\"https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11310507846892748812026/artifact/icon.png\" alt=\"do_11310507846892748812026\" data-asset-variable=\"do_11310507846892748812026\"></figure>"}]',
'[{"id":"do_1127129497561497601326","type":"image","src":"/content/do_1127129497561497601326.img/artifact/sunbird_1551961194254.jpeg","baseUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev"}]',
'{"response1": {"maxScore": 1, "cardinality": "single", "type": "integer", "correctResponse": { "value": "1", "outcomes": {"SCORE": 1}}}}'
);


INSERT INTO dev_question_store.question_data(identifier, body, editorstate, answer, solutions, media, responsedeclaration, interactions) VALUES (
'do_113188615625731.img',
textAsBlob('<div class="question-body"><div class="mcq-title"><p>Color of Sky is?</p></div><div data-choice-interaction="response1" class="mcq-vertical"></div></div>'),
'{"question":"<p>Capital of india is?</p>","answer":"<p>New Delhi</p>"}',
textAsBlob('1'),
'[{"id": "c012a8a9-f78b-6ddb-3ac2-bd1f38c7850b", "type": "html", "value": "<p>Color of sky is blue&nbsp;</p><figure class=\"image\"><img src=\"https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11310507846892748812026/artifact/icon.png\" alt=\"do_11310507846892748812026\" data-asset-variable=\"do_11310507846892748812026\"></figure>"}]',
'[{"id": "do_11318931140144332811620", "type": "image", "src": "https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11318931140144332811620/artifact/i.png", baseUrl": "https://dock.sunbirded.org"}, {"id": "do_11310507846892748812026", "type": "image", "src": "https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11310507846892748812026/artifact/icon.png", "baseUrl": "https://dock.sunbirded.org"}]',
'{"response1": {"maxScore": 1, "cardinality": "single", "type": "integer", "correctResponse": { "value": "1", "outcomes": {"SCORE": 1}}}}',
'{"response1": {"type": "choice","options": [{"label": "<p>Red&nbsp;</p><figure class=\"image\"><img src=\"https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11318931140144332811620/artifact/i.png\" alt=\"do_11318931140144332811620\" data-asset-variable=\"do_11318931140144332811620\"></figure>","value": 0}]}}'
);


INSERT INTO dev_hierarchy_store.questionset_hierarchy(identifier, hierarchy) VALUES ('do_123', '{ "name": "QS1", "identifier": "do_123", "children": [{"identifier": "do_124", "objectType": "QuestionSet", "visibility": "Parent"}]}');
INSERT INTO dev_hierarchy_store.questionset_hierarchy(identifier, hierarchy) VALUES ('do_321', '{ "name": "QS1", "identifier": "do_123", "children": [{"identifier": "do_124", "objectType": "QuestionSet", "visibility": "Parent"}]}');
INSERT INTO dev_hierarchy_store.questionset_hierarchy(identifier, hierarchy) VALUES ('do_123.img', '{ "name": "QS1", "identifier": "do_123", "children": [{"identifier": "do_124", "objectType": "QuestionSet", "visibility": "Parent"}, {"identifier": "do_125", "objectType": "QuestionSet", "visibility": "Parent"}]}');