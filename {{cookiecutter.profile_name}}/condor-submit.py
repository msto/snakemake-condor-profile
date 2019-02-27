#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2019 Matthew Stone <mrstone3@wisc.edu>
# Distributed under terms of the MIT license.

"""

"""

import argparse
import tempfile
import os
import textwrap
import shutil
import subprocess

from snakemake.utils import read_job_properties


def parse_condor_submit_info(res):
    info = dict()

    stdout = res.stdout.split(b'\n')
    for line in stdout:
        if line.startswith(b'** Proc'):
            info['jobid'] = line.split()[-1].strip(b':').decode('UTF-8')

    return info


def condor_submit(jobscript):
    job_properties = read_job_properties(jobscript)

    with tempfile.TemporaryDirectory() as jobdir:
        sub_path = os.path.join(jobdir, 'job.sub')

        # TODO: add job properties as params
        with open(sub_path, 'w') as subfile:
            subfile.write(textwrap.dedent("""
                executable      = jobscript.sh
                log             = jobscript.log
                output          = jobscript.out
                error           = jobscript.err
                queue
                """)

        shutil.copyfile(jobscript, os.path.join(jobdir, 'jobscript.sh'))

        workdir = os.getcwd()
        os.chdir(jobdir)
        cmd = ['condor_submit', subfile.name]

        for i in range(10):
            try:
                res = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)
                break
            except subprocess.CalledProcessError as e:
                if i >= 9:
                    raise e

        info = parse_condor_submit_info(res)
        os.chdir(workdir)

    print(res['jobid'])


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('jobscript')
    args = parser.parse_args()

    condor_submit(args.jobscript)


if __name__ == '__main__':
    main()
