#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright © 2019 Matthew Stone <mrstone3@wisc.edu>
# Distributed under terms of the MIT license.

"""

"""

import argparse
import subprocess
import getpass


def is_running(jobid):
    """
    Check if job ID is in list of currently running jobs from condor_q
    """

    # TODO make username configurable
    username = getpass.getuser()
    cmd1 = ["condor_q", "-sub", username]
    cmd2 = ["sed", "-e", r"0,/SUBMITTED/d"]
    cmd3 = ["sed", "-n", "-e", r"/^$/q;p"]
    cmd4 = ["awk", "{print $1}"]

    p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(cmd2, stdout=subprocess.PIPE, stdin=p1.stdout)
    p3 = subprocess.Popen(cmd3, stdout=subprocess.PIPE, stdin=p2.stdout)
    p4 = subprocess.Popen(cmd4, stdout=subprocess.PIPE, stdin=p3.stdout)

    output = p4.communicate()[0].decode('UTF-8')
   
    running_jobs = output.split('\n')
    
    return jobid in running_jobs


def parse_condor_history(res):
    """
    Scrape runtime states from condor_history -l

    - for now, just check exit status
    - later versions of condor support json output but not 8.4
    """

    info = dict()
    stdout = res.stdout.split(b'\n')
    for line in stdout:
        if line.startswith(b'ExitStatus'):
            info['ExitStatus'] = int(line.split()[-1])

    return info


def check_exit_status(jobid):
    """
    Check if job exited successfully
    """

    cmd = ["condor_history", "-l", jobid]
    res = subprocess.run(cmd, check=True, stdout=subprocess.PIPE)

    info = parse_condor_history(res)

    return info['ExitStatus']


def condor_status(jobid):
    if is_running(jobid):
        print("running")
    elif check_exit_status(jobid) == 0:
        print("success")
    else:
        print("failed")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('jobid')
    args = parser.parse_args()

    condor_status(args.jobid)


if __name__ == '__main__':
    main()
