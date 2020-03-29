INSERT INTO thor.descriptors (name, active, data_source, data_set, seconds)
VALUES ('Generico', TRUE, '{
    "prefix": "upsert_master/upsert_method/",
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
          "lamb_func": "lambda record: utils.return_format(record[''IDENTIDAD''].replace('','','' ''))"
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
    "CIUDAD": {
      "columns_file_descriptor": [
        "CIUDAD"
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
]'::JSON, 1)
