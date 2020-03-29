SELECT
    d.descriptor_id, d.name, d.active, d.data_source, d.data_set, d.seconds, FALSE AS kill,
    ARRAY_AGG(JSON_BUILD_OBJECT('credentials', c.name,
                      'database', d2.schema_name,
                      'table', d2.table_name,
                      'source', d2.source,
                      'primary_key', d2.primary_key,
                      'destination_descriptor', d2.data_set)) AS data_destination
FROM
    thor.descriptors d INNER JOIN
    thor.descriptor_destinations dd USING(descriptor_id) INNER JOIN
    thor.destinations d2 USING(destination_id) INNER JOIN
    thor.credentials c USING(credential_id)
GROUP BY
    1, 2, 3, d.data_source::TEXT, d.data_set::TEXT, 6, 7;
