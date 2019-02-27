#!/usr/bin/env python3
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
#  from snakemake.logging import format_wildcards

def format_wildcards(wildcards, key_order=None):
    """
    Convert wildcard dict to string suitable for filename suffix

    (Snakemake built-in doesn't work with Condor because it includes equal
    signs)
    """

    # Alphabetically sort keys if no other order specified
    if key_order is None:
        key_order = sorted(wildcards.keys())

    return ','.join(['{0}_{1}'.format(k, wildcards[k]) for k in key_order])


def parse_condor_submit_info(res):
    """
    Parse submission info from condor_submit. For now, just scrape job ID
    """

    info = dict()

    stdout = res.stdout.split(b'\n')
    for line in stdout:
        if line.startswith(b'** Proc'):
            info['jobid'] = line.split()[-1].strip(b':').decode('UTF-8')

    return info


def condor_submit(jobscript):
    """
    Submit snakemake jobscript to Condor.

    Creates `condor` directory in working directory, where job submission files
    and log files are stored.
        - condor/sub/rule.wildcards.sub : submission file
        - condor/log/rule.wildcards.log : Condor log
        - condor/out/rule.wildcards.out : Job stdout
        - condor/err/rule.wildcards.err : Job stderr
    """
    job_properties = read_job_properties(jobscript)

    # Create condor log directories if necessary
    for logdir in 'sub log out err'.split():
        sub_dir = os.path.join(os.getcwd(), 'condor', logdir)
        os.makedirs(sub_dir, exist_ok=True)

    # TODO: add Condor params to job properties
    # TODO: parameterize location of `condor` directory
    wildcard_str = format_wildcards(job_properties['wildcards'])
    sub_path = os.path.join(os.getcwd(), 'condor', 'sub', '{rule}.{wildcards}.sub')
    sub_path = sub_path.format(rule=job_properties['rule'],
                               wildcards=wildcard_str)

    with open(sub_path, 'w') as subfile:
        subfile.write(textwrap.dedent("""
            executable      = {jobscript}
            log             = condor/log/{rule}.{wildcards}.log
            output          = condor/out/{rule}.{wildcards}.out
            error           = condor/err/{rule}.{wildcards}.err
            queue
            """).format(jobscript=jobscript, 
                        rule=job_properties['rule'],
                        wildcards=wildcard_str))

    # Submit job to condor
    cmd = ['condor_submit', '-verbose', subfile.name]
    res = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)

    # Get job ID back
    info = parse_condor_submit_info(res)

    print(info['jobid'])


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('jobscript')
    args = parser.parse_args()

    condor_submit(args.jobscript)


if __name__ == '__main__':
    main()
