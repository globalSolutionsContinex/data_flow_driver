CREATE TABLE master (
    product_id varchar(32) NOT NULL,
    ean varchar(26) NOT NULL,
    name varchar(256),
    brand varchar(128),
    reference varchar(256),
    model varchar(128),
    description varchar(4096),
    features json,
    images json,
    videos json,
    PRIMARY KEY(product_id)
);

CREATE TABLE master_process_1 (
    product_id varchar(32) NOT NULL,
    PRIMARY KEY(product_id)
);

CREATE TABLE master_state (
    product_id varchar(32) NOT NULL,
    id_thor varchar(32) NOT NULL,
    file_origin varchar(256),
    state smallint,
    message varchar(4096),
    update_date date,
    creation_date date,
    PRIMARY KEY(product_id)
);