DROP TABLE IF EXISTS thor.descriptor_destinations;
CREATE TABLE thor.descriptor_destinations (
    descriptor_destination_id   SERIAL,
    descriptor_id               INT,
    destination_id              INT
);

ALTER TABLE thor.descriptor_destinations
    ADD CONSTRAINT descriptor_destinations_pk
        PRIMARY KEY (descriptor_destination_id);

ALTER TABLE thor.descriptor_destinations
    ADD CONSTRAINT descriptor_descriptor_destinations_fk
        FOREIGN KEY (descriptor_id) REFERENCES thor.descriptors(descriptor_id);

ALTER TABLE thor.descriptor_destinations
    ADD CONSTRAINT descriptor_destinations_destination_id_fk
        FOREIGN KEY (destination_id) REFERENCES thor.destinations(destination_id);

CREATE INDEX descriptor_destinations_descriptor_id_index
    ON thor.descriptor_destinations(descriptor_id);
