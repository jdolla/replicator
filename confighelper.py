import json
import sys
import multiprocessing as mp
from os import path


class config():

    def __init__(self, args, configF):
        self._args = args

        if 'jobs' not in configF:
            raise KeyError('No jobs found in config file.')

        self._jobConfigs = configF['jobs']
        self._global = configF['global'] if 'global' in configF else {}

    @property
    def auto(self):
        if self._args.auto:
            return self._args.auto

        if (self._global and 'auto' in self._global and
                self._global['auto']):
            return self._global['auto']

        return False

    @property
    def batch(self):
        if self._args.batch:
            return self._args.batch

        if self._global and 'batch' in self._global and self._global['batch']:
            return self._global['batch']

        return 10000

    @property
    def proc(self):
        maxProc = max(1, mp.cpu_count() - 2)

        if self._args.proc:
            return min(int(self._args.proc), maxProc)

        if (self._global and 'proc' in self._global and
                self._global['proc']):
            return self._global['proc']

        return maxProc

    @property
    def commit(self):
        if self._args.commit:
            return self._args.commit

        if (self._global and 'commit' in self._global and
                self._global['commit']):
            return self._global['commit']

        return 500

    @property
    def jobs(self):
        """
        Parses the config json and retuns jobs
        """

        jconf = self._jobConfigs
        reqJobs = self._args.jobs

        if reqJobs:
            notFound = [job for job in reqJobs if job not in jconf]

            if notFound:
                raise ValueError(
                    f'Invalid job(s) specified: {", ".join(notFound)}')

        if not reqJobs:
            reqJobs = [job for job in jconf]

        jobs = {}
        for k, v in jconf.items():
            if k not in reqJobs:
                continue

            srcConnStr = self.connStr(v['source'])
            trgtConnStr = self.connStr(v['target'])

            for table in v['tables']:
                srcSchema = table['source']['schema']
                srcTable = table['source']['name']

                trgtSchema = table['target']['schema']
                trgtTable = table['target']['name']

                jobs[f"{k}.{srcSchema}.{srcTable}"] = {
                    "source": {
                        'connStr': srcConnStr,
                        'schema': srcSchema,
                        'name': srcTable,
                    },
                    "target": {
                        'connStr': trgtConnStr,
                        'schema': trgtSchema,
                        'name': trgtTable,
                    }
                }

        return jobs

    def connStr(self, parts):
        """
        Parses the connection string parts and returns a full
        connection string.

        Note:   if a full connection string is specified then all other
                attributes are ignored.
        """
        if 'connStr' in parts and parts['connStr']:
            return parts['connStr']

        if 'port' in parts and parts['port']:
            port = f",{parts['port']}"
        else:
            port = ""

        connStr = f"DRIVER={{{parts['driver']}}};" \
            f"SERVER={parts['host']}{port};" \
            f"DATABASE={parts['database']};"

        if 'trusted' in parts and parts['trusted']:
            connStr += "Trusted_Connection=yes;"

        if 'username' in parts:
            connStr += f"UID={parts['username']};"

        if 'password' in parts:
            connStr += f"PWD={parts['password']};"

        return connStr
