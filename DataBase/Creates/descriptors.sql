DROP TABLE IF EXISTS thor.descriptors CASCADE;
CREATE TABLE thor.descriptors (
    descriptor_id   SERIAL,
    name            VARCHAR(31),
    active          BOOLEAN,
    data_source     JSON,
    data_set        JSON,
    seconds         INTEGER
);

ALTER TABLE thor.descriptors
    ADD CONSTRAINT descriptors_pk
        PRIMARY KEY (descriptor_id);

CREATE INDEX descriptors_name_index
    ON thor.descriptors(name);
