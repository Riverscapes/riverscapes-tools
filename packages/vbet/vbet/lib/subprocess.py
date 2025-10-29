from __future__ import annotations
from typing import List
import subprocess
from rsxml import Logger


def runrscli(cwd: str, cmd: List[str]):
    log = Logger('Running RSCli')
    log.info('Running command: {}'.format(' '.join(cmd)))

    # Realtime logging from subprocess
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd, env={'NO_UI': 'Garfield'})
    # Here we print the lines in real time but we will also log them afterwords
    # replace '' with b'' for Python 3
    for output in iter(process.stdout.readline, b''):
        for line in output.decode('utf-8').split('\n'):
            if len(line) > 0:
                log.info(line)

    for errout in iter(process.stderr.readline, b''):
        for line in errout.decode('utf-8').split('\n'):
            if len(line) > 0:
                log.error(line)

    retcode = process.poll()
    if retcode > 0:
        log.error('Process returned with code {}'.format(retcode))

    return retcode


runrscli('/mnt/e/Data', ['rscli', 'download', '/mnt/e/Data/17060304_DELTEME', '--id', '953c4550-585b-493c-ae5b-5108f263ed67s', '--no-input'])
