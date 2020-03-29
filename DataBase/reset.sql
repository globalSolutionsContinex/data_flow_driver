DROP TABLE IF EXISTS thor.credentials CASCADE;
CREATE TABLE thor.credentials
(
    credential_id SERIAL,
    name          VARCHAR(31),
    specs         BYTEA
);

ALTER TABLE thor.credentials
    ADD CONSTRAINT credentials_pk
        PRIMARY KEY (credential_id);

CREATE INDEX mappers_credentials_index
    ON thor.credentials (name);

DROP TABLE IF EXISTS thor.descriptors CASCADE;
CREATE TABLE thor.descriptors
(
    descriptor_id SERIAL,
    name          VARCHAR(31),
    active        BOOLEAN,
    data_source   JSON,
    data_set      JSON,
    seconds       INTEGER
);

ALTER TABLE thor.descriptors
    ADD CONSTRAINT descriptors_pk
        PRIMARY KEY (descriptor_id);

CREATE INDEX descriptors_name_index
    ON thor.descriptors (name);

DROP TABLE IF EXISTS thor.destinations CASCADE;
CREATE TABLE thor.destinations
(
    destination_id SERIAL,
    credential_id  INT,
    schema_name    VARCHAR(31),
    primary_key    VARCHAR(64),
    source         VARCHAR(31),
    table_name     VARCHAR(31),
    data_set       JSON
);

ALTER TABLE thor.destinations
    ADD CONSTRAINT destinations_pk
        PRIMARY KEY (destination_id);

ALTER TABLE thor.destinations
    ADD CONSTRAINT destinations_credential_id_fk
        FOREIGN KEY (credential_id) REFERENCES thor.credentials (credential_id);

DROP TABLE IF EXISTS thor.descriptor_destinations;
CREATE TABLE thor.descriptor_destinations
(
    descriptor_destination_id SERIAL,
    descriptor_id             INT,
    destination_id            INT
);

ALTER TABLE thor.descriptor_destinations
    ADD CONSTRAINT descriptor_destinations_pk
        PRIMARY KEY (descriptor_destination_id);

ALTER TABLE thor.descriptor_destinations
    ADD CONSTRAINT descriptor_descriptor_destinations_fk
        FOREIGN KEY (descriptor_id) REFERENCES thor.descriptors (descriptor_id);

ALTER TABLE thor.descriptor_destinations
    ADD CONSTRAINT descriptor_destinations_destination_id_fk
        FOREIGN KEY (destination_id) REFERENCES thor.destinations (destination_id);

CREATE INDEX descriptor_destinations_descriptor_id_index
    ON thor.descriptor_destinations (descriptor_id);

INSERT INTO thor.credentials (name, specs)
VALUES ('database', pgp_sym_encrypt(
        '{"hostname":"cartera-bd.cjxcsq7qqmym.us-east-1.rds.amazonaws.com",
        "username":"cartera",
        "password":"p2LFt5k9mPQSfzIprc",
        "database":"thor"}', 'i1ua2xt2huBVVFq4ImwNgxd3UWf8V813')),
       ('s3', pgp_sym_encrypt(
               '{"bucketName":"thor-files",
               "accessKey":"AKIAWCI2MGMN5K23K2UZ",
               "secretKey":"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"}', 'i1ua2xt2huBVVFq4ImwNgxd3UWf8V813'));

INSERT INTO thor.descriptors (name, active, data_source, data_set, seconds)
VALUES ('Generico', TRUE, '{
  "prefix": "master/upsert/",
  "suffix": ".json"
}', '[
  {
    "FECHA": {
      "columns_file_descriptor": [
        "FECHA"
      ]
    },
    "TIPO_DOC": {
      "columns_file_descriptor": [
        "TIPO_DOC"
      ]
    },
    "NUMERO_DOC": {
      "columns_file_descriptor": [
        "NUMERO_DOC"
      ]
    },
    "CUENTA": {
      "columns_file_descriptor": [
        "CUENTA"
      ]
    },
    "NOMBRE": {
      "columns_file_descriptor": [
        "NOMBRE"
      ]
    },
    "CONCEPTO": {
      "columns_file_descriptor": [
        "CONCEPTO"
      ]
    },
    "IDENTIDAD": {
      "columns_file_descriptor": [
        "IDENTIDAD"
      ],
      "rules": [
        {
          "lamb_func": "lambda record: utils.return_format(record[''IDENTIDAD''].replace('','',''''))"
        }
      ]
    },
    "DV": {
      "columns_file_descriptor": [
        "DV"
      ]
    },
    "TELEFONO": {
      "columns_file_descriptor": [
        "TELEFONO"
      ]
    },
    "DIRECCION": {
      "columns_file_descriptor": [
        "DIRECCION"
      ]
    },
    "CENTRO_COSTO": {
      "columns_file_descriptor": [
        "CENTRO_COSTO"
      ]
    },
    "DEBITO": {
      "columns_file_descriptor": [
        "DEBITO"
      ],
      "rules": [
        {
          "lamb_func": "lambda product: utils.return_format(int(product[''DEBITO'']))"
        }
      ]
    },
    "CREDITO": {
      "columns_file_descriptor": [
        "CREDITO"
      ],
      "rules": [
        {
          "lamb_func": "lambda product: utils.return_format(int(product[''CREDITO'']))"
        }
      ]
    },
    "file_origin": {
      "columns_file_descriptor": [
        "file_origin"
      ]
    }
  }
]'::JSON, 1);

INSERT INTO thor.destinations (credential_id, primary_key, source, schema_name, table_name, data_set)
VALUES (1, 'id', 'postgresql', 'public', 'master', '{
  "id": {
    "columns_file_descriptor": [
      "FECHA",
      "TIPO_DOC",
      "NUMERO_DOC",
      "CUENTA",
      "DIRECCION",
      "CONCEPTO",
      "IDENTIDAD",
      "CENTRO_COSTO",
      "DEBITO"
    ],
    "columns_pattern": "{}-{}-{}-{}-{}-{}-{}-{}-{}",
    "rules": [
      {
        "lamb_func": "lambda record: utils.return_format(utils.get_hashmd5(record[''id''], ''utf-8''))"
      }
    ]
  },
  "fecha": {
    "columns_file_descriptor": [
      "FECHA"
    ]
  },
  "tipo_doc": {
    "columns_file_descriptor": [
      "TIPO_DOC"
    ]
  },
  "numero_doc": {
    "columns_file_descriptor": [
      "NUMERO_DOC"
    ]
  },
  "cuenta": {
    "columns_file_descriptor": [
      "CUENTA"
    ]
  },
  "primer_apellido": {
    "columns_file_descriptor": [
      "NOMBRE"
    ],
    "rules": [
      {
        "lamb_func": "lambda record: utils.return_format('''' if utils.is_company(record[''identidad''],record[''dv'']) else utils.get_name(record[''primer_apellido''],0))"
      }
    ]
  },
   "segundo_apellido": {
    "columns_file_descriptor": [
      "NOMBRE"
    ],
     "rules": [
       {
         "lamb_func": "lambda record: utils.return_format('''' if utils.is_company(record[''identidad''],record[''dv'']) else utils.get_name(record[''segundo_apellido''],1))"
       }
     ]
  },
   "primer_nombre": {
    "columns_file_descriptor": [
      "NOMBRE"
    ],
     "rules": [
       {
         "lamb_func": "lambda record: utils.return_format('''' if utils.is_company(record[''identidad''],record[''dv'']) else utils.get_name(record[''primer_nombre''],2))"
       }
     ]
  },
   "segundo_nombre": {
    "columns_file_descriptor": [
      "NOMBRE"
    ],
     "rules": [
       {
         "lamb_func": "lambda record: utils.return_format('''' if utils.is_company(record[''identidad''],record[''dv'']) else utils.get_name(record[''segundo_nombre''],3))"
       }
     ]
  },
   "empresa": {
    "columns_file_descriptor": [
      "NOMBRE"
    ],
     "rules": [
       {
         "lamb_func": "lambda record: utils.return_format(record[''empresa''] if utils.is_company(record[''identidad''],record[''dv'']) else '''')"
       }
     ]
  },
  "concepto": {
    "columns_file_descriptor": [
      "CONCEPTO"
    ]
  },
  "identidad": {
    "columns_file_descriptor": [
      "IDENTIDAD"
    ]
  },
  "dv": {
    "columns_file_descriptor": [
      "DV"
    ]
  },
  "telefono": {
    "columns_file_descriptor": [
      "TELEFONO"
    ]
  },
  "ciudad": {
    "default": ""
  },
  "centro_costo": {
    "columns_file_descriptor": [
      "CENTRO_COSTO"
    ]
  },
  "direccion": {
    "columns_file_descriptor": [
      "DIRECCION"
    ]
  },
  "debito": {
    "columns_file_descriptor": [
      "DEBITO"
    ]
  },
  "credito": {
    "columns_file_descriptor": [
      "CREDITO"
    ]
  },
  "file_origin": {
    "columns_file_descriptor": [
      "file_origin"
    ]
  }
}');

INSERT INTO thor.descriptor_destinations (descriptor_id, destination_id)
VALUES (1, 1);


