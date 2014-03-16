#!/usr/bin/env python

import argparse, os, re, select, stat, subprocess, sys, tempfile
from util import *

# global variables
usage = "cat list_of_commands.txt | ssub -n 100 -q hour -G gscidfolk -m 8 --io 10\n\
  ssub -n 100 -q week -G broadfolk -m 8 --io 10 'command1; command2; command3;'"

# configure parameters for the cluster
username = 'csmillie'
mode = 'LSF'
temp_dir = '/home/unix/csmillie/tmp'
default_q = 'hour'
default_G = 'gscidfolk'
default_m = 4
default_n = 150
default_io = 1
header = '#!/bin/bash\nsource /home/unix/%s/.bashrc\n' %(username)

# get submit and stat commands
if mode == 'LSF':
    submit_cmd = 'bsub'
    stat_cmd = 'bjobs -w'
elif mode == 'PBS':
    submit_cmd = 'qsub'
    stat_cmd = 'qstat -l'

def parse_args():
    # parse command line arguments
    parser = argparse.ArgumentParser(usage = usage)
    parser.add_argument('-n', default = default_n, type = int, help = 'number of cpus')
    parser.add_argument('-q', default = default_q, help = 'queue')
    parser.add_argument('-G', default = default_G, help = 'group')
    parser.add_argument('-m', default = default_m, help = 'memory (gb)')
    parser.add_argument('--io', default = default_io, help = 'disk io (units)')
    parser.add_argument('commands', nargs = '?', default = '')
    args = parser.parse_args()
    return args


def parse_job(out, mode=mode):
    # parse job_id from the output of submit_cmd
    if mode == 'LSF':
        job_id = re.search('Job <(\d+)>', out).group(1)
    elif mode == 'PBS':
        job_id = out.rstrip()
    return job_id


def job_status():
    # return job status (e.g. bjobs -w)
    process = subprocess.Popen([stat_cmd], stdout = subprocess.PIPE, shell=True)
    [out, err] = process.communicate()
    out = [line for line in out.split('\n') if stat_username in line]
    return [out, err]


def n_jobs():
    # calculate the number of running jobs
    return len(job_status())


def jobs_finished(job_ids):
    # check if jobs are finished
    [out, err] = job_status()
    for job in out:
        for job_id in job_ids:
            if job_id in job:
                return False
    return True


def mktemp(suffix='.tmp', dir='', header=''):
    # make temporary file and return [filehandle, filename]
    fh, fn = tempfile.mkstemp(suffix='.sh', dir=temp_dir)
    os.close(fh)
    fh = open(fn, 'w')
    fh.write(header)
    return fh, fn


def write_jobs(args, commands):
    # write job scripts from a list of commands
    
    # initialize output files
    fhs, fns = zip(*[mktemp(suffix='.sh', dir=temp_dir, header=header) for i in range(args.n)])
    fhs_cycle = cycle(fhs)
    
    # write commands to file
    for command in commands:
        fh = fhs_cycle.next()
        fh.write('%s\n' %(command))
    
    # close all filehandles
    for fh in fhs:
        fh.close()
    
    # make executable and print message
    for fn in fns:
        os.chmod(fn, stat.S_IRWXU)
        message('Writing job %s' %(fn))
    
    return fns


def submit_jobs(fns):
    # submit jobs to the cluster
    job_ids = []
    for fn in fns:
        process = subprocess.Popen(['%s < %s' %(submit_cmd, fn)], stdout = subprocess.PIPE, shell=True)
        [out, error] = process.communicate()
        job_ids.append(parse_job(out))
        message('Submitting job %s' %(fn))
    return job_ids


def write_LSF_array(args, fns):
    # write an LSF job array from args and filenames
    
    # initialize output file
    fh, array_fn = mktemp(suffix='.sh', dir=temp_dir, header=header)
    array_fn = os.path.abspath(fn)
    
    # write header
    fh.write('#BSUB -J "job[1-%d]"\n' %(len(fns)))
    fh.write('#BSUB -e %s.e.%%I\n' %(array_fn))
    fh.write('#BSUB -o %s.o.%%I\n' %(array_fn))
    fh.write('#BSUB -q %s\n' %(args.q))
    fh.write('#BSUB -G %s\n' %(args.G))
    fh.write('#BSUB -R "rusage[mem=%s:argon_io=%s]"\n' %(args.m, args.io))
    fh.write('cd $LS_SUBCWD\n')
    
    # write job array
    for i, fn in enumerate(fns):
        os.chmod(fn, stat.S_IRWXU)
        fh.write('job_array[%d]=%s\n' %(i+1, os.path.abspath(fn)))
    fh.write('${job_array[$LSB_JOBINDEX]};\n')
    fh.close()
    
    # make executable and print message
    os.chmod(array_fn, stat.S_IRWXU)
    message('Writing array %s' %(array_fn))
    return array_fn


def write_PBS_array(args, fns):
    # write a PBS job array from args and filenames
    
    # initialize output file
    fh, array_fn = mktemp(suffix='.sh', dir=temp_dir, header=header)
    array_fn = os.path.abspath(fn)
    
    # write header
    fh.write('#PBS -t 1-%d\n' %(len(fns)))
    fh.write('#PBS -e %s.e\n' %(array_fn))
    fh.write('#PBS -o %s.o\n' %(array_fn))
    fh.write('#PBS -q %s\n' %(args.q))
    fh.write('#PBS -l pmem=%sgb\n' %(args.m))
    fh.write('cd $PBS_O_WORKDIR\n')
    
    # write job array
    for i, fn in enumerate(fns):
        os.chmod(fn, stat.S_IRWXU)
        fh.write('job_array[%d]=%s\n' %(i+1, os.path.abspath(fn)))
    fh.write('${job_array[$PBS_ARRAYID]};\n')
    fh.close()
    
    # make executable and print message
    os.chmod(array_fn, stat.S_IRWXU)
    message('Writing array %s' %(array_fn))
    return array_fn


def write_job_array(args, fns, mode):
    # write a job array (LSF or PBS)
    if mode == 'LSF':
        array_fn = write_LSF_array(args, fns)
    elif mode == 'PBS':
        array_fn = write_PBS_array(args, fns)
    return array_fn


def submit_array(args, commands):
    # submit a job array to the cluster
    fns = write_jobs(args, commands)
    array_fn = write_LSF_array(args, fns)
    job_id = submit_jobs([array_fn])[0]
    message('Submitting job array %s' %(array_fn))
    return job_id


def wait_for_jobs(job_ids):
    # wait for jobs to finish
    while True:
        time.sleep(5)
        if jobs_finished(job_ids):
            break


def submit_pipeline(args, pipeline):
    # a pipeline is a list of lists of commands
    for commands in pipeline:
        job_ids = submit_array(args, commands)
        wait_for_jobs(job_ids)


def initialize():
    # initialize global variables for ssub
    
    # parse command line args
    args = parse_args()
    
    # get list of commands
    commands = []
    if args.commands != '':
        commands += [command.strip() for command in args.commands.split(';')]
    if select.select([sys.stdin], [], [], 0)[0]:
        commands += [line.rstrip() for line in sys.stdin.readlines()]
    
    # calculate number of cpus
    if args.n < 0:
        args.n = len(commands)


initialize()

if __name__ == '__main__':
    submit_job_array(args, commands)


    