INSERT INTO thor.destinations (credential_id, source, schema_name, table_name, data_set) VALUES
(1,'postgresql', 'catalog', 'master', '{
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
      ]
    },
    "CREDITO": {
      "columns_file_descriptor": [
        "CREDITO"
      ]
    },
    "file_origin": {
      "columns_file_descriptor": [
        "file_origin"
      ]
    }
  }'),
(1,'postgresql', 'catalog', 'master_process', '{
        "product_id": {
            "columns_file_descriptor": ["CUENTA", "IDENTIDAD"],
            "columns_pattern": "{}-{}",
            "rules": [
                {
                    "lamb_func": "lambda product: utils.return_format(utils.get_hashmd5(product[''product_id''], ''utf-8''))"
                }
            ]
        }
  }'),
(1,'postgresql', 'catalog', 'master_state', '{
        "product_id": {
            "columns_file_descriptor": ["CUENTA", "IDENTIDAD"],
            "columns_pattern": "{}-{}",
            "rules": [
                {
                    "lamb_func": "lambda product: utils.return_format(utils.get_hashmd5(product[''product_id''], ''utf-8''))"
                }
            ]
        },
        "id_thor": {
            "columns_file_descriptor": ["id_thor"]
        },
        "file_origin": {
            "columns_file_descriptor": ["file_origin"]
        },
        "state": {
            "default": 0
        },
        "message": {
            "default": "thor_update"
        },
        "update_date": {
            "default": "0",
            "rules": [
                {
                    "lamb_func": "lambda product: utils.return_format(utils.now())"
                }
            ]
        },
        "creation_date": {
            "default": "0",
            "rules": [
                {
                    "lamb_func": "lambda product: utils.return_format(utils.now())"
                }
            ]
        }
}');
