DROP TABLE IF EXISTS thor.mappers;
CREATE TABLE thor.mappers (
    mapper_id   INT,
    name        VARCHAR(31),
    description VARCHAR(127)
);

ALTER TABLE thor.mappers
    ADD CONSTRAINT mappers_pk
        PRIMARY KEY (mapper_id);

CREATE INDEX mappers_name_index
    ON thor.mappers(name);
