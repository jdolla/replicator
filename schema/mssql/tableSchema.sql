SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

Declare @Schema SYSNAME = ?;
Declare @Table SYSNAME = ?;

select sc.ORDINAL_POSITION
    , sc.COLUMN_NAME
    , sc.IS_NULLABLE
    , sc.DATA_TYPE
    , sc.CHARACTER_MAXIMUM_LENGTH
    , sc.NUMERIC_PRECISION
    , sc.NUMERIC_SCALE
from INFORMATION_SCHEMA.columns sc
where sc.table_schema = @Schema
    and sc.TABLE_NAME = @Table
order by sc.ORDINAL_POSITION asc;