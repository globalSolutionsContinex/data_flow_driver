DROP TABLE IF EXISTS thor.mapper_values;
CREATE TABLE thor.mapper_values (
    mapper_value    SERIAL,
    mapper_id       INT,
    key             VARCHAR(31),
    value           VARCHAR(127)
);

ALTER TABLE thor.mapper_values
    ADD CONSTRAINT mapper_values_pk
        PRIMARY KEY (mapper_value);

ALTER TABLE thor.mapper_values
    ADD CONSTRAINT mapper_mapper_values_fk
        FOREIGN KEY (mapper_id) REFERENCES thor.mappers (mapper_id);

CREATE INDEX mapper_values_mapper_id_index
    ON thor.mapper_values(mapper_id);
