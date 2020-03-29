create schema thor;
DROP TABLE IF EXISTS thor.credentials;
CREATE TABLE thor.credentials (
    credential_id   SERIAL,
    name            VARCHAR(31),
    specs           BYTEA
);

ALTER TABLE thor.credentials
    ADD CONSTRAINT credentials_pk
        PRIMARY KEY (credential_id);

CREATE INDEX mappers_credentials_index
    ON thor.credentials(name);
