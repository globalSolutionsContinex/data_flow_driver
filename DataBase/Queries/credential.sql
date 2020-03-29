SELECT
    pgp_sym_decrypt(specs, '{key}')::JSON
FROM
    thor.credentials
WHERE
    name = '{credential_name}';
