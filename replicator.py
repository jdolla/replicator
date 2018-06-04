import sys
import time
import multiprocessing as mp
import logging
import logging.config
import replicatorLogConfig
import traceback
import pyodbc
import argparse
import replicatorConfig as rc
from mssql.table import Table as sqlTable
from collections import deque

log = logging.getLogger('replicator')


def log_uncaught_exceptions(ex_cls, ex, tb):
    log.critical(''.join(traceback.format_tb(tb)))
    log.critical('{0}: {1}'.format(ex_cls, ex))
    sys.exit(1)


def procDataflow(commons, conf):
    """
    Processes a dataflow.
        src: a data source (table)
        trgt: a data target (table)

    Note:   It is possible to extend this pattern by including
            transformation functions.
            Someting to think about for later :)
    """
    print('hello!')
    time.sleep(10)
    # srcTable = sqlTable(
    #     connection=pyodbc.connect(src['connStr']),
    #     schemaName=src['schema'],
    #     tableName=src['name']
    # )

    # trgtTable = sqlTable(
    #     connection=pyodbc.connect(trgt['connStr']),
    #     schemaName=trgt['schema'],
    #     tableName=trgt['name']
    # )

    # trgtTable.syncWith(srcTable)  # need to add somethin' bout auto
    # trgtTable.batch = 10

    # rowSets = srcTable.rows(trgtTable.rowver(), 500)

    # while True:
    #     for rowSet in rowSets:
    #         trgtTable.merge(rowSet, srcTable.columns)

    #     rowSets = srcTable.rows(trgtTable.rowver())

    #     if not rowSets:
    #         break


def nextProc(runQueue, runJobs, commons):
    jobName = runQueue.popleft()
    jobConf = runJobs[jobName]
    jobArgs = (commons, jobConf)
    jobProc = mp.Process(target=procDataflow, name=jobName, args=jobArgs)
    return jobProc


def main(args):
    config = rc.config(args)
    runJobs = config.jobs
    runQueue = deque([k for k in runJobs])

    commons = {
        'batch': config.batch,
        'auto': config.auto,
        'commit': config.commit
    }

    dfProcs = []
    for _ in range(min(config.proc, len(runQueue))):
        jobProc = nextProc(runQueue, runJobs, commons)
        jobProc.start()
        dfProcs.append(jobProc)

    while len(dfProcs) > 0:
        for dfProc in dfProcs:
            if dfProc.is_alive() is False:
                dfProcs.remove(dfProc)
                log.debug(f'Process ended {dfProc.name}')

                if dfProc.exitcode != 0:
                    log.error(f'Process failed: {dfProc}')

                runQueue.append(dfProc.name)
                jobProc = nextProc(runQueue, runJobs, commons)
                jobProc.start()
                log.debug(f'Process started {jobProc.name}.')
                dfProcs.append(jobProc)

            time.sleep(5)


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

    parser.add_argument('-b', '--batch', nargs='?',
                        help='<Optional> Size of batch to retrieve from source.',
                        required=False)

    parser.add_argument('-c', '--commit', nargs='?',
                        help='<Optional> Size of batch to commit to target.',
                        required=False)

    parser.add_argument('-a', '--auto', action="store_true",
                        help='<Optional> Auto create target tables.',
                        required=False)

    parser.add_argument('-d', '--debug', action='store_true',
                        help='<Optional> Use debug logging level.',
                        required=False)

    args = parser.parse_args()

    if args.debug:
        replicatorLogConfig.LOGGING['loggers']['replicator']['level'] = 'DEBUG'

    # configure the logging
    logging.config.dictConfig(replicatorLogConfig.LOGGING)

    # log unhandled exceptions
    sys.excepthook = log_uncaught_exceptions

    log.debug('entering main()')
    sys.exit(main(args))
