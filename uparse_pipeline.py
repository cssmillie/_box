import sys, os, os.path, subprocess, qsub

# USEARCH path
usearch = '/home/csmillie/bin/usearch7.0.1001_i86linux32'

# Get forward and reverse reads
f = sys.argv[1] # forward fastq
r = sys.argv[2] # reverse fastq
P = sys.argv[3] # forward primer
Q = sys.argv[4] # reverse primer
b = sys.argv[5] # barcodes
x = sys.argv[6] # index fastq
k = int(sys.argv[7]) # divide into k files
z = int(sys.argv[8]) # 0 = print, 1 = submit

def error(message):
    print message

def srun(cmd, z):
    if type(cmd) == type(''):
        if z == 0:
            print cmd
        else:
            os.system(cmd)
    elif type(cmd) == type([]):
        if z == 0:
            print '\n'.join(cmd)
        else:
            for c in cmd:
                os.system(cmd)
    else:
        error('type(cmd) is unknown')

def qrun(cmds, z):
    if z == 0:
        print '\n'.join(cmds)
    else:
        qsub_and_wait(cmds)


# Split forward and reverse reads
cmds = []
cmds.append('python /home/csmillie/sbin/fasta/split_fastq.py %s %d' %(f,k))
cmds.append('python /home/csmillie/sbin/fasta/split_fastq.py %s %d' %(r,k))
qrun(cmds, z)

# Compress forward and reverse fastq, check tgz and remove
cmd = 'tar -cvzf raw.fsq.tgz %s %s' %(f, r)
qrun([cmd], z)
try:
    test = subprocess.check_output(['gunzip', '-t', 'raw.fsq.tgz'])
    if test != '':
        error('error: gunzip -t raw.fsq.tgz')
except:
    error('error: gunzip -t raw.fsq.tgz')

cmd = 'rm %s %s' %(f, r)
srun(cmd, z)

# Get filenames of split
fi = ['%s.%d.fsq' %(os.path.splitext(os.path.basename(f))[0],i) for i in range(k)]
ri = ['%s.%d.fsq' %(os.path.splitext(os.path.basename(r))[0],i) for i in range(k)]
mi = ['merge.%d.fsq' %(i) for i in range(k)]
Fi = ['%s.%d.tmp' %(os.path.splitext(os.path.basename(f))[0],i) for i in range(k)]
Ri = ['%s.%d.tmp' %(os.path.splitext(os.path.basename(r))[0],i) for i in range(k)]
Mi = ['merge.%d.tmp' %(i) for i in range(k)]

# Make sure split files exist
if False in [os.path.exists(fn) for fn in (fi+ri)]:
    error('error: split files do not exist')

# Remove diversity regions and primer, and discard reads with > 2 mismatches
cmds = []
for i in range(k):
    cmds.append( 'python /home/csmillie/sbin/otus/remove_primer.py %s %s > %s' %(fi[i], P, Fi[i]) )
    cmds.append( 'python /home/csmillie/sbin/otus/remove_primer.py %s %s > %s' %(ri[i], Q, Ri[i]) )
qrun(cmds, z)

# Make sure tmp files exist and clean up
if False in [os.path.exists(fn) for fn in (Fi+Ri)]:
    error('error: remove_primer.py files do not exist')
cmds = []
for i in range(k):
    cmds.append( 'mv %s %s' %(Fi[i], fi[i]) )
    cmds.append( 'mv %s %s' %(Ri[i], ri[i]) )
srun(cmds, z)

# Intersect forward and reverse reads
cmds = []
for i in range(k):
    cmd = 'python /home/csmillie/sbin/otus/intersect.py %s %s %s %s' %(fi[i], ri[i], Fi[i], Ri[i])
    cmds.append(cmd)
qrun(cmds, z)

# Make sure intersect files exist and clean up
if False in [os.path.exists(fn) for fn in (Fi+Ri)]:
    error('error: intersect.py files do not exist')
cmds = []
for i in range(k):
    cmds.append('mv %s %s' %(Fi[i], fi[i]))
    cmds.append('mv %s %s' %(Ri[i], ri[i]))
srun(cmds, z)

# Merge forward and reverse reads
cmds = []
for i in range(k):
    cmds.append( '%s -fastq_mergepairs %s -reverse %s -fastq_truncqual 5 -fastqout %s' %(usearch, fi[i], ri[i], mi[i]) )
qrun(cmds, z)

# Make sure merged files exist and clean up
if False in [os.path.exists(fn) for fn in mi]:
    error('error: merged reads do not exist')
cmds = []
for i in range(k):
    cmds.append( 'rm %s %s' %(fi[i], ri[i]) )
srun(cmds, z)

# Quality filter with maximum expected error = .5
cmds = []
for i in range(k):
    cmds.append( '%s -fastq_filter %s -fastq_maxee .5 -fastaout %s' %(usearch, mi[i], Mi[i]) )
qrun(cmds, z)

# Make sure qf files exist and clean up
if False in [os.path.exists(fn) for fn in Mi]:
    error('error: quality filter files do not exist')
cmds = []
for i in range(k):
    cmds.append( 'mv %s %s' %(Mi[i], mi[i]) )
srun(cmds, z)

# Map index -> barcodes and relabel reads
cmds = []
for i in range(k):
    cmds.append( 'python /home/csmillie/sbin/otus/map_barcodes.py %s %s %s > qf.%d.fst' %(mi[i], b, x, i) )
qrun(cmds, z)

# Make sure files exist and cleanup
if False in [os.path.exists('qf.%d.fst' %(i)) for i in range(k)]:
    error('map_barcodes.py files do not exist')
cmds = []
for i in range(k):
    cmds.append( 'rm %s' %(mi[i]) )
srun(cmds, z)

# Concatenate files and dereplicate
srun('cat qf.[0-9]*.fst > qf.fst', z)
srun('rm qf.[0-9]*.fst', z)
srun('python /home/csmillie/sbin/otus/derep_fulllength.py qf.fst qf.derep.fst', z)

# Cluster at 90, 95, 97, and 99% identity
cmds = []
for sid in '90 95 97 99'.split():
    cmds.append( '%s -cluster_otus qf.derep.fst -otus otus.%s.fst -otuid .%s' %(usearch, sid, sid) )
qrun(cmds, z)

# Chimera filter
cmds = []
for sid in '90 95 97 99'.split():
    cmds.append( '%s -uchime_ref otus.%s.fst -db /home/csmillie/db/gold.broad.fa -nonchimeras otus.%s.tmp -strand plus' %(usearch, sid, sid))
qrun(cmds, z)

# Clean up
cmds = []
for sid in '90 95 97 99'.split():
    cmds.append( 'mv otus.%s.tmp otus.%s.fst' %(sid, sid) )
srun(cmds, z)

# Relabel fasta file
cmds = []
for sid in '90 95 97 99'.split():
    cmds.append( 'python /home/csmillie/bin/usearch_python/fasta_number.py otus.%s.fst OTU%s_ > otus.%s.tmp' %(sid, sid, sid) )
qrun(cmds, z)

# Clean up
cmds = []
for sid in '90 95 97 99'.split():
    cmds.append( 'mv otus.%s.tmp otus.%s.fst' %(sid, sid) )
srun(cmds, z)

# Map reads back to OTUs
cmds = []
for sid in '90 95 97 99'.split():
    cmds.append('%s -usearch_global qf.fst -db otus.%s.fst -strand plus -id .%s -uc otus.%s.uc' %(usearch, sid, sid, sid))
qrun(cmds, z)

# Construct OTU table
cmds = []
for sid in '90 95 97 99'.split():
    cmds.append( 'python /home/csmillie/bin/usearch_python/uc2otutab.py otus.%s.uc otus.%s.tab' %(sid, sid))
qrun(cmds, z)

# Transpose tables
cmds = []
for sid in '90 95 97 99'.split():
    cmds.append( 'python /home/csmillie/sbin/data/transpose.py otus.%s.tab > otus.%s.counts' %(sid, sid))
qrun(cmds, z)

# Normalize
cmds = []
for sid in '90 95 97 99'.split():
    cmds.append( 'python /home/csmillie/sbin/data/norm.py -i otus.%s.counts -o otus.%s.norm' %(sid, sid))
    cmds.append( 'python /home/csmillie/sbin/data/norm.py -i otus.%s.counts -o otus.%s.log -log 1 -pseudo 1e-6' %(sid, sid))
qrun(cmds, z)

