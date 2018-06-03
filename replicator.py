import sys
import time
import multiprocessing as mp
import pyodbc
import argparse
import replicatorConfig as rc
from mssql.table import Table as sqlTable


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


def main(args):

    try:
        config = rc.config(args)
    except FileNotFoundError as err:
        print(f'Unable to open config file:\n{err}')
        sys.exit(1)
    except Exception as err:
        print(f'There was a problem loading the configuration data.\n'
              f'error: {err}')
        sys.exit(1)

    try:
        runJobs = config.jobs
    except Exception as err:
        print(f'There was a problem loading the job list:\nerror:{err}')

    for job, configs in runJobs.items():
        print('Running Job:', job, configs)


if __name__ == '__main__':
    epilog = "All replicator arguments are optional and will" \
        " override the values in the config file."

    parser = argparse.ArgumentParser(
        description='Data Replicator', epilog=epilog)
    parser.add_argument('-j', '--jobs', nargs='+',
                        help='<Optional> List of jobs to process.'
                        ' If omitted all jobs will process.',
                        required=False)

    parser.add_argument('-p', '--proc', nargs='?',
                        help='<Optional> Number of processes to use.',
                        required=False)

    args = parser.parse_args()
    sys.exit(main(args))
