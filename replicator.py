from mssql.table import Table as sqlTable
import pyodbc
import json
import time
import multiprocessing as mp
import configHelper as ch


def procDataflow(src, trgt):
    """
    Processes a dataflow.
        src: a data source (table)
        trgt: a data target (table)

    Note:   It is possible to extend this pattern by including
            transformation functions.
            Someting to think about for later :)
    """

    srcTable = sqlTable(
        connection=pyodbc.connect(src['connStr']),
        schemaName=src['schema'],
        tableName=src['name']
    )

    trgtTable = sqlTable(
        connection=pyodbc.connect(trgt['connStr']),
        schemaName=trgt['schema'],
        tableName=trgt['name']
    )

    trgtTable.syncWith(srcTable)
    trgtTable.batch = 10

    rowSets = srcTable.rows(trgtTable.rowver(), 500)

    while True:
        for rowSet in rowSets:
            trgtTable.merge(rowSet, srcTable.columns)

        rowSets = srcTable.rows(trgtTable.rowver())

        if not rowSets:
            break


if __name__ == '__main__':
    """
    Process Data Flows
    """

    with open('replicator.config.json', 'r') as f:
        config = json.load(f)

    for k, v in config.items():
        srcConnStr = ch.getConnStr(v['source'])
        trgtConnStr = ch.getConnStr(v['target'])
        print(srcConnStr)
# {
#     connStr:xxx,
#     schema:xxx,
#     name:xxx
# }
# project = config['demo']

# sourceTable = sqlTable(
#     connection=pyodbc.connect(project['source']['connStr'], timeout=30),
#     schemaName=project['tables'][0]['source']['schema'],
#     tableName=project['tables'][0]['source']['name'])

# targetTable = sqlTable(
#     connection=pyodbc.connect(project['target']['connStr'], timeout=30),
#     schemaName=project['tables'][0]['target']['schema'],
#     tableName=project['tables'][0]['target']['name'])


# targetTable.syncWith(sourceTable)
# targetTable.batch = 10
# sourceRows = sourceTable.rows(targetTable.rowver(), 500)

# while True:
#     for row in sourceRows:
#         targetTable.merge(row, sourceTable.columns)

#     sourceRows = sourceTable.rows(targetTable.rowver())
#     time.sleep(10)
