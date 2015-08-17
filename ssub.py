#!/usr/bin/env python

import argparse, os, os.path, re, select, stat, subprocess, sys, tempfile, time
from util import *

'''
ssub is a simple job submission script

usage:
  cat list_of_commands.txt | ssub -n 100 -q hour -G gscidfolk -m 8 --io 10
  ssub -n 100 -q week -G broadfolk -m 8 --io 10 "command1; command2; command3;"

to use it as a python library:
  import ssub
  ssub.args.n = 100
  ssub.args.q = "hour"
  commands = get_commands()
  job_ids = submit_array(ssub.args, commands)
  wait_for_jobs(job_ids)
  print "done"

to create and run pipelines:
  import ssub
  ssub.args.q = "week"
  ssub.args.G = "broadfolk"
  commands1 = run_blast()
  commands2 = parse_blast()
  pipeline = [commands1, commands2]
  submit_pipeline(args, pipeline)
  print "done"

'''


# set global variables
username = 'csmillie'
cluster = 'broad'

# sun grid engine header
def sge_header(n_jobs, max_jobs=250, outfile='error', queue='short', memory=None, array=False):
    h = '''
    #!/bin/bash
    source ~/.bashrc
    source /broad/software/scripts/useuse
    '''
    if array == True:
        h += '''
        #$ -t 1-%d
        #$ -tc %d
        #$ -j y
        #$ -o %s
        #$ -q %s
        #$ -cwd
        ''' %(n_jobs, max_jobs, outfile, queue)
        if memory is not None:
            h += '\n#$ -l m_mem_free=%dg' %(memory)
    h = re.sub('\n\s+', '\n', h)
    return h

# torque header
def pbs_header(n_jobs, max_jobs=250, outfile='error', queue='short', memory=None, array=False):
    h = '''
    #!/bin/bash
    source ~/.bashrc
    '''
    if array == True:
        h += '''
        #PBS -t 1-%d%%%d
        #PBS -j oe
        #PBS -o %s
        #PBS -q %s
        cd $PBS_O_WORKDIR
        ''' %(n_jobs, max_jobs, outfile, queue)
        if memory is not None:
            h += '\n#PBS -l mem=%dgb' %(memory)
    h = re.sub('\n\s+', '\n', h)
    return h


def parse_args():
    
    # print usage statement
    usage = "cat list_of_commands.txt | ssub -n 100 -q hour -G gscidfolk -m 8 --io 10\n"
    usage +="ssub -n 100 -q long -m 8 'command 1; command 2; command 3;"
    
    # add command line arguments
    parser = argparse.ArgumentParser(usage = usage)
    parser.add_argument('-n', default=250, type=int, help='number of cpus')
    parser.add_argument('-q', default='short', help='queue')
    parser.add_argument('-m', default=0, type=int, help='memory (gb)')
    parser.add_argument('-o', default='error', help='outfile')
    parser.add_argument('commands', nargs='?', default='')
    
    # parse arguments from stdin
    if __name__ == '__main__':
        args = parser.parse_args()
    else:
        args = parser.parse_args('')
    
    return args


class Submitter():
    
    def __init__(self, cluster):
        
        # get command line arguments
        args = parse_args()
        
        # initialize cluster parameters
        self.cluster = cluster
        self.username = username
        self.n = args.n
        self.m = args.m
        self.q = args.q
        self.o = args.o
        self.commands = args.commands
        
        if self.cluster == 'broad':
            self.header = sge_header
            self.submit_cmd = 'qsub'
            self.parse_job = lambda x: re.search('Your job-array (\d+)', x).group(1)
            self.stat_cmd = 'qstat'
            self.task_id = '$SGE_TASK_ID'
        
        if self.cluster == 'coyote':
            self.header = pbs_header
            self.submit_cmd = 'qsub'
            self.parse_job = lambda x: x.rstrip()
            self.stat_cmd = 'qstat -l'
            self.task_id = '$PBS_ARRAYID'
        
        if self.m == 0:
            self.m = None
    
    
    def get_header(self, commands, array=False):
        h = self.header(n_jobs = len(commands), max_jobs=self.n, outfile=self.o, queue=self.q, memory=self.m, array=array)
        return h
    
    
    def mktemp(self, commands, prefix='tmp', suffix='.tmp', array=False):
        # make temporary file and return [fh, fn]
        fh, fn = tempfile.mkstemp(dir=os.getcwd(), prefix=prefix, suffix=suffix)
        os.close(fh)
        fh = open(fn, 'w')
        fh.write(self.get_header(commands=commands, array=array))
        fn = os.path.abspath(fn)
        return fh, fn
    
    
    def job_status(self):
        # return job status
        process = subprocess.Popen([self.stat_cmd], stdout = subprocess.PIPE, shell=True)
        [out, err] = process.communicate()
        out = [line for line in out.split('\n') if self.username in line]
        return [out, err]
    
    
    def jobs_finished(self, job_ids):
        # check if jobs are finished
        [out, err] = self.job_status()
        for job in out:
            for job_id in job_ids:
                if job_id in job:
                    return False
        return True
    
    
    def submit_jobs(self, fns):
        # submit jobs to the cluster
        job_ids = []
        for fn in fns:
            process = subprocess.Popen(['%s %s' %(self.submit_cmd, fn)], stdout = subprocess.PIPE, shell=True)
            [out, error] = process.communicate()
            job_ids.append(self.parse_job(out))
            message('Submitting job %s' %(fn))
        return job_ids
    
    
    def write_array(self, commands):
        
        # write jobs
        fh1, fn1 = self.mktemp(commands, prefix='jobs.', suffix='.sh', array=False)
        for i, command in enumerate(commands):
            fh1.write('job_array[%d]=%s\n' %(i+1, command))
        fh1.write('${job_array[$1]}\n')
        fh1.close()
        os.chmod(fn1, stat.S_IRWXU)
        
        # write array
        fh2, fn2 = self.mktemp(commands, prefix='array.', suffix='.sh', array=True)
        fh2.write('%s %s\n' %(fn1, self.task_id))
        fh2.close()
        os.chmod(fn2, stat.S_IRWXU)
        
        # message
        message('Writing jobs %s and array %s' %(fn1, fn2))
        return fn2
    
    
    def submit(self, commands, out=False):
        # submit a job array to the cluster
        if out == False:
            array_fn = self.write_array(commands)
            job_ids = self.submit_jobs([array_fn])
            return job_ids
        elif out == True:
            print '\n'.join(commands)
            return []
    
    
    def wait(self, job_ids, out=False):
        # wait for jobs to finish
        if out == False:
            while True:
                time.sleep(5)
                if self.jobs_finished(job_ids):
                    break 
    
    
    def submit_and_wait(self, commands, out=False):
        # submit job array and wait for it to finish
        job_ids = self.submit(commands, out = out)
        self.wait(job_ids, out = out)
    
    
    def submit_pipeline(self, pipeline, out=False):
        # a pipeline is a list of lists of commands
        for commands in pipeline:
            self.submit_and_wait(commands, out=out)
    
    


def get_commands():
    ssub = Submitter(cluster)
    if ssub.commands != '':
        commands = [command.strip() for command in ssub.commands.split(';')]
    else:
        commands = [line.rstrip() for line in sys.stdin.readlines()]    
    return commands


if __name__ == '__main__':
    ssub = Submitter(cluster)
    commands = get_commands()
    ssub.submit(commands)

