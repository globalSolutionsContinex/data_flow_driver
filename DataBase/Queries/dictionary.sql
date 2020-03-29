SELECT
    key, value
FROM
    thor.mappers m INNER JOIN
    thor.mapper_values mv USING(mapper_id)
WHERE
    m.name = '{mapper_name}';
