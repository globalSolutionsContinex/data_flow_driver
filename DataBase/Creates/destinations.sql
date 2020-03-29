DROP TABLE IF EXISTS thor.destinations CASCADE;
CREATE TABLE thor.destinations (
    destination_id  SERIAL,
    credential_id   INT,
    schema_name     VARCHAR(31),
    source     VARCHAR(31),
    table_name      VARCHAR(31),
    data_set        JSON
);

ALTER TABLE thor.destinations
    ADD CONSTRAINT destinations_pk
        PRIMARY KEY (destination_id);

ALTER TABLE thor.destinations
    ADD CONSTRAINT destinations_credential_id_fk
        FOREIGN KEY (credential_id) REFERENCES thor.credentials(credential_id);
