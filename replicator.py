import sys
import time
import multiprocessing as mp
import threading
import logging
import logging.config
import logging.handlers
import traceback
import pyodbc
import argparse
import confighelper as cfgh
from mssql.table import Table as sqlTable
from collections import deque
import json


def log_uncaught_exceptions(ex_cls, ex, tb):
    rLog.critical(''.join(traceback.format_tb(tb)))
    rLog.critical('{0}: {1}'.format(ex_cls, ex))
    logQ.put(None)
    sys.exit(1)


def logger_thread(logQ):
    while True:
        record = logQ.get()
        if record is None:
            break
        logger = logging.getLogger('replicator')
        logger.handle(record)


def procDataflow(commons, conf):
    """
    Processes a dataflow.
        src: a data source (table)
        trgt: a data target (table)

    Note:   It is possible to extend this pattern by including
            transformation functions.
            Someting to think about for later :)
    """
    dfName = mp.current_process().name
    dfPid = mp.current_process().pid
    qh = logging.handlers.QueueHandler(commons['logQ'])
    root = logging.getLogger()
    root.setLevel(commons['lvl'])
    root.addHandler(qh)
    dfLogger = logging.getLogger('replicator')

    dfLogger.debug(f'({dfPid}) {dfName}: Running dataflow processes.')

    try:

        dfLogger.debug(f'({dfPid}) {dfName}: connecting to source db.')
        srcTable = sqlTable(
            connection=pyodbc.connect(conf['source']['connStr']),
            schemaName=conf['source']['schema'],
            tableName=conf['source']['name']
        )

        dfLogger.debug(f'({dfPid}) {dfName}: connecting to target db.')
        trgtTable = sqlTable(
            connection=pyodbc.connect(conf['target']['connStr']),
            schemaName=conf['target']['schema'],
            tableName=conf['target']['name']
        )

        dfLogger.debug(f'({dfPid}) {dfName}: synching table.')
        trgtTable.syncWith(srcTable, commons['auto'])
        dfLogger.debug(f'({dfPid}) {dfName}: Completed dataflow processes.')

        rowSets = srcTable.rows(trgtTable.rowver(), commons['commit'])
        for rowSet in rowSets:
            if rowSet:
                trgtTable.merge(rowSet, srcTable.columns)

        time.sleep(1)  # take a nap...

    except:
        e = sys.exc_info()
        ex = f'({dfPid}) {dfName}: {e[1]}'
        dfLogger.critical(''.join(traceback.format_tb(e[2])))
        dfLogger.critical('{0}: {1}'.format(e[0], ex))


def nextProc(runQueue, runJobs, commons):
    jobName = runQueue.popleft()
    jobConf = runJobs[jobName]
    jobArgs = (commons, jobConf)
    jobProc = mp.Process(target=procDataflow, name=jobName, args=jobArgs)
    return jobProc


def main(args, logQ):
    config = cfgh.config(args)
    runJobs = config.jobs
    runQueue = deque([k for k in runJobs])

    commons = {
        'batch': config.batch,
        'auto': config.auto,
        'commit': config.commit,
        'logQ': logQ,
        'lvl': logging.DEBUG if args.debug else logging.ERROR
    }

    rLog.debug(f'{"*" * 20}')
    rLog.debug(f'batch size: {config.batch}')
    rLog.debug(f'commit size: {config.commit}')
    rLog.debug(f'auto commit: {config.auto}')
    rLog.debug(f'{"*" * 20}')

    dfProcs = []
    for _ in range(min(config.proc, len(runQueue))):
        jobProc = nextProc(runQueue, runJobs, commons)
        jobProc.start()
        dfProcs.append(jobProc)

    while len(dfProcs) > 0:
        for dfProc in dfProcs:
            if dfProc.is_alive() is False:
                dfProcs.remove(dfProc)
                runQueue.append(dfProc.name)
                jobProc = nextProc(runQueue, runJobs, commons)
                jobProc.start()
                dfProcs.append(jobProc)

            time.sleep(5)  # pause before double-checking

    logQ.put(None)


# Logging objects for main process
rLog = logging.getLogger('replicator')
logQ = mp.Queue()


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

    with open('logconfig.json', 'r') as f:
        logCfg = json.load(f)

    print('Loading log configuration.')

    if args.debug:
        logCfg['logging']['loggers']['replicator']['level'] = 'DEBUG'

    # configure the logging
    logging.config.dictConfig(logCfg['logging'])

    # setup a logging thread
    logProc = threading.Thread(target=logger_thread, args=(logQ,))
    logProc.start()

    # log unhandled exceptions
    sys.excepthook = log_uncaught_exceptions

    print('Starting replicator jobs')
    sys.exit(main(args, logQ))
