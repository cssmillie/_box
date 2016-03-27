#!/usr/bin/env python

import argparse, os, os.path, re, stat, subprocess, sys, tempfile, time

'''
usage:
  cat list_of_commands.txt | ssub -n 100 -q short -m 8
  ssub -q long -m 8 "command1; command2; command3;"
'''


# set global variables
username = 'csmillie'
cluster = 'broad' # options = broad,amazon,coyote

# amazon header - sun grid engine
def amazon_header(n_jobs, error='error', queue='short', memory=None, array=False):
    h = '''#!/bin/bash
    source ~/.bashrc
    '''
    if array == True:
        h += '''
        #$ -t 1-%d
        #$ -j y
        #$ -o %s
        #$ -cwd
        ''' %(n_jobs, error, queue)
    h = re.sub('\n\s+', '\n', h)
    return h

# broad header - univa grid engine
def broad_header(n_jobs, error='error', queue='short', memory=None, array=False):
    h = '''#!/bin/bash
    source ~/.bashrc
    '''
    if array == True:
        h += '''
        #$ -t 1-%d
        #$ -j y
        #$ -o %s
        #$ -q %s
        #$ -cwd
        ''' %(n_jobs, error, queue)
        if memory is not None:
            h += '\n#$ -l m_mem_free=%dg\n' %(memory)
    h = re.sub('\n\s+', '\n', h)
    return h

# coyote header - torque
def coyote_header(n_jobs, error='error', queue='short', memory=None, array=False):
    h = '''#!/bin/bash
    source ~/.bashrc
    '''
    if array == True:
        h += '''
        #PBS -t 1-%d%%250
        #PBS -j oe
        #PBS -o %s
        #PBS -q %s
        cd $PBS_O_WORKDIR
        ''' %(n_jobs, error, queue)
        if memory is not None:
            h += '\n#PBS -l mem=%dgb' %(memory)
    h = re.sub('\n\s+', '\n', h)
    return h


class EmptyParser:
    def __init__(self):
        self.q = 'short'
        self.m = 0
        self.o = 'error'
        self.commands = []


def parse_args():

    if __name__ == '__main__':
        
        # print usage statement
        usage = "\n\n  cat list_of_commands.txt | ssub -q long -m 8 -o test\n"
        usage +="  ssub -q short -m 16 'command 1; command 2; command 3;"
        
        # add command line arguments
        parser = argparse.ArgumentParser(usage = usage)
        parser.add_argument('-q', default='short', help='queue')
        parser.add_argument('-m', default=0, type=int, help='memory (gb)')
        parser.add_argument('-o', default='tmp', help='output prefix')
        parser.add_argument('-p', default=False, action='store_true', help='print commands')
        parser.add_argument('commands', nargs='?', default='')

        # parse arguments from stdin
        args = parser.parse_args()
        
    else:
        args = EmptyParser()
    
    return args


class Submitter():
    
    def __init__(self, cluster=cluster):
        
        # get command line arguments
        args = parse_args()
       
        # cluster parameters
        self.cluster = cluster
        self.username = username
        self.m = args.m
        self.q = args.q
        self.o = args.o
        self.p = args.p
        self.commands = args.commands
        
        if self.cluster == 'broad':
            self.header = broad_header
            self.submit_cmd = 'qsub'
            self.parse_job = lambda x: re.search('Your job-array (\d+)', x).group(1)
            self.stat_cmd = 'qstat -g d'
            self.task_id = '$SGE_TASK_ID'
        
        if self.cluster == 'coyote':
            self.header = coyote_header
            self.submit_cmd = 'qsub'
            self.parse_job = lambda x: x.rstrip()
            self.stat_cmd = 'qstat -l'
            self.task_id = '$PBS_ARRAYID'

        if self.cluster == 'amazon':
            self.header = amazon_header
            self.submit_cmd = 'qsub'
            self.parse_job = lambda x: re.search('Your job-array (\d+)', x).group(1)
            self.stat_cmd = 'qstat -g d'
            self.task_id = '$SGE_TASK_ID'
        
        if self.m == 0:
            self.m = None
    
    
    def get_header(self, commands, error='error', array=False):
        h = self.header(n_jobs = len(commands), error=error, queue=self.q, memory=self.m, array=array)
        return h
    
    def get_njobs(self):
        [out, err] = self.job_status()
        njobs = len(out)
        return njobs
    
    
    def mktemp(self, commands, prefix='tmp', suffix='.tmp', array=False):
        # make temporary file and return [fh, fn]
        fh, fn = tempfile.mkstemp(dir=os.getcwd(), prefix=prefix, suffix=suffix)
        os.close(fh)
        fh = open(fn, 'w')
        fh.write(self.get_header(commands=commands, error='%s.err' %(fn), array=array))
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
            job = job.split()[0] # job is the first column of qstat
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
            print 'Submitting job %s' %(fn)
        return job_ids
    
    
    def write_array(self, commands):
        
        # write jobs
        fh1, fn1 = self.mktemp(commands, prefix='%s.' %(self.o), suffix='.j.sh', array=False)
        for i, command in enumerate(commands):
            fh1.write('job_array[%d]="%s"\n' %(i+1, command))
        fh1.write('eval ${job_array[$1]}\n')
        fh1.close()
        os.chmod(fn1, stat.S_IRWXU)
        print 'Writing jobs %s' %(fn1)
        
        # write array
        fn2 = re.sub('j.sh$', 'a.sh', fn1)
        fh2 = open(fn2, 'w')
        fh2.write(self.get_header(commands=commands, error='%s.err' %(fn2), array=True))
        fh2.write('%s %s\n' %(fn1, self.task_id))
        fh2.close()
        os.chmod(fn2, stat.S_IRWXU)
        print 'Writing array %s' %(fn2)
        
        return fn2
    
    
    def submit(self, commands):
        # submit a job array to the cluster
        if len(commands) == 0:
            return []
        if type(commands) == str:
            commands = [commands]
        if self.p == False:
            array_fn = self.write_array(commands)
            job_ids = self.submit_jobs([array_fn])
            return job_ids
        elif self.p == True:
            print '\n'.join(commands)
            return []
    
    
    def wait(self, job_ids=[]):
        # wait for jobs to finish
        if self.p == False:
            while True:
                time.sleep(5)
                if self.jobs_finished(job_ids):
                    break
    
    def submit_and_wait(self, commands):
        # submit job array and wait for it to finish
        job_ids = self.submit(commands)
        self.wait(job_ids=job_ids)
    
    
    def submit_pipeline(self, pipeline):
        # a pipeline is a list of lists of commands
        for commands in pipeline:
            self.submit_and_wait(commands)
    
    


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

