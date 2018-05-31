SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

Declare @Schema SYSNAME = ?;
Declare @Table SYSNAME = ?;

Declare @objectId bigint = object_id(concat(@Schema, '.', @Table))

select c.name as COLUMN_NAME
from sys.columns c
inner join sys.indexes i
    on c.object_id = i.object_id
left join sys.index_columns ic
    on ic.object_id = i.object_id
    and ic.index_id = i.index_id
    and c.column_id = ic.column_id
where i.object_id = @objectId
    and is_primary_key = 1
order by ic.key_ordinal asc
    , COLUMNPROPERTY(c.object_id, c.name, 'ordinal')
