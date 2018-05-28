SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

Declare @Schema SYSNAME = ?;
Declare @Table SYSNAME = ?;

Declare @objectId bigint = object_id(concat(@Schema, '.', @Table))

select c.name
from sys.indexes i
inner join sys.index_columns ic
    on ic.object_id = i.object_id
    and ic.index_id = i.index_id
inner join sys.columns c
    on c.object_id = i.object_id
    and c.column_id = ic.column_id
where i.object_id = @objectId
    and is_primary_key = 1
order by ic.key_ordinal