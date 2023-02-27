CREATE KEYSPACE dialcodes WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '2'}  AND durable_writes = true;

CREATE TABLE dialcodes.dialcode_images (
    filename text PRIMARY KEY,
    channel text,
    config map<text, text>,
    created_on timestamp,
    dialcode text,
    publisher text,
    status int,
    url text
);

CREATE TABLE dialcodes.dialcode_batch (
    processid uuid PRIMARY KEY,
    channel text,
    config map<text, text>,
    created_on timestamp,
    dialcodes list<text>,
    publisher text,
    status int,
    url text
);

INSERT INTO dialcodes.dialcode_images(filename, channel, dialcode, url) VALUES ('0_Q1I5I3', 'b00bc992ef25f1a9a8d63291e20efc8d', 'Q1I5I3', 'https://sunbirddev.blob.core.windows.net/sunbird-content-dev/in.ekstep/0_Q1I5I3.png') ;
