INSERT INTO @schema.@table_name(
    @column_definition
)

SELECT
    @column_definition
FROM 
    @schema.@temp_table_name
    