SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

Declare @Schema SYSNAME = ?;
Declare @Table SYSNAME = ?;

select sc.COLUMN_NAME
from INFORMATION_SCHEMA.columns sc
where sc.table_schema = @Schema
    and sc.TABLE_NAME = @Table
order by sc.ORDINAL_POSITION asc;