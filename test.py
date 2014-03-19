import argparse, ssub
from util import *

# usearch path
usearch = 'usearch'
ggdb = '/home/csmillie/'

def parse_args():
    
    # create argument parser
    parser = argparse.ArgumentParser()
    
    # add groups
    group1 = parser.add_argument_group('Pipeline')
    group2 = parser.add_argument_group('Input files')
    group3 = parser.add_argument_group('Remove primers')
    group4 = parser.add_argument_group('Merge reads')
    group5 = parser.add_argument_group('Demultiplex')
    group6 = parser.add_argument_group('Quality filtering')
    group7 = parser.add_argument_group('Dereplicate')
    group8 = parser.add_argument_group('Chimeras')
    group9 = parser.add_argument_group('Clustering')
    group10 = parser.add_argument_group('Options')
    
    # add arguments
    group1.add_argument('--all', default = False, action = 'store_true', help = 'Run all steps of pipeline?')
    group1.add_argument('--primers', default = False, action = 'store_true', help = 'Remove primers?')
    group1.add_argument('--merge', default = False, action = 'store_true', help = 'Merge forward and reverse reads?')
    group1.add_argument('--demultiplex', default = False, action = 'store_true', help = 'Demultiplex?')
    group1.add_argument('--qfilter', default = False, action = 'store_true', help = 'Quality filter?')
    group1.add_argument('--chimeras', default = False, action = 'store_true', help = 'Chimera slay?')
    group1.add_argument('--denovo', default = False, action = 'store_true', help = 'Denovo clustering (UPARSE)?')
    group1.add_argument('--ref_gg', default = False, action = 'store_true', help = 'Reference mapping (Greengenes)?')
    group2.add_argument('-f', help = 'Input fastq (forward)')
    group2.add_argument('-r', help = 'Input fastq (reverse)')
    group2.add_argument('-p', help = 'Primer fastq (forward)')
    group2.add_argument('-q', help = 'Primer fastq (reverse)')
    group2.add_argument('-b', help = 'Barcodes list')
    group2.add_argument('-x', help = 'Index fastq')
    group3.add_argument('--p_mismatch', default = 1, type = int, help = 'Number of mismatches allowed in primers')
    group5.add_argument('--b_mismatch', default = 1, type = int, help = 'Number of mismatches allowed in barcodes')
    group6.add_argument('--truncqual', default = 2, type = int, help = '')
    group6.add_argument('--maxee', default = 2., type = float, help = 'Maximum expected error (UPARSE)')
    group8.add_argument('--gold_db', default = '~/db/gold.broad.fa', help = 'Gold 16S database')
    group9.add_argument('--sids', default='91,94,97,99', help='Sequence identities for clustering')
    group10.add_argument('-n', default = 1, type = int, help = 'Number of CPUs')
    group10.add_argument('-z', default = False, action = 'store_true', help = 'Print output commands')
    
    # parse arguments
    if __name__ == '__main__':
        args = parser.parse_args()
    else:
        args = parser.parse_args('')
    
    # process arguments
    args.sids = map(int, args.sids.split(','))
        
    return args


def get_filenames(args):
    
    # Generate filenames to use in pipeline
    f_base = os.path.basename(args.f)
    r_base = os.path.basename(args.r)
    args.fi = ['%s.%d' %(args.f, i) for i in range(args.n)] # forward reads (split)
    args.ri = ['%s.%d' %(args.r, i) for i in range(args.n)] # reverse reads (split)
    args.mi = ['%s.%d.merge' %(args.f, i) for i in range(args.n)] # merged reads (split)
    args.Fi = ['%s.%d.tmp' %(args.f, i) for i in range(args.n)] # forward reads (temp)
    args.Ri = ['%s.%d.tmp' %(args.r, i) for i in range(args.n)] # reverse reads (temp)
    args.Mi = ['%s.%d.tmp' %(args.m, i) for i in range(args.n)] # merged reads (temp)
    args.ci = ['q.%d.fst' %(f_base, i) for i in range(args.n)] # current reads
    args.Ci = ['q.%d.tmp' %(r_base, i) for i in range(args.n)] # current reads (temp)
    args.oi = ['otus.%d.fst' %(sid) for sid in args.sids] # otu representative sequences
    args.Oi = ['otus.%d.tmp' %(sid) for sid in args.sids] # otu representative sequences (temp)
    args.uc = ['otus.%d.uc' %(sid) for sid in args.sids] # uclust output files
    args.xi = ['otus.%d.counts' %(sid) for sid in args.sids] # otu tables (counts)
    
    # Get database for read mapping
    if args.denovo == True:
        args.db = args.oi
    elif args.gg_ref == True:
        args.db = ['%s/rep_set/gg_%d_otus_4feb2011.fasta' %(ggdb, sid) for sid in args.sids]
    
    return args


def split_fastq(args):
    # Split forward and reverse reads (for parallel processing)
    
    # Get list of commands
    cmds = []
    if args.f:
        cmd = 'python ~/box/split_fastq.py %s %s' %(args.f, args.n)
        cmds.append(cmd)
    if args.r:
        cmd = 'python ~/box/split_fastq.py %s %s' %(args.r, args.n)
        cmds.append(cmd)
    
    # Submit commands and validate output
    ssub.submit_and_wait(cmds, args.z)
    ssub.validate_output(args.fi + args.ri, args.z)


def remove_primers(args):
    # Remove diversity region + primer and discard reads with > 2 mismatches
    
    # Get list of commands
    cmds = []
    for i in range(args.n):
        if args.f:
            cmd = '~/box/remove_primer.py %s %s > %s' %(args.fi[i], args.p, args.Fi[i])
            cmds.append(cmd)
        if args.r:
            cmd = '~/box/remove_primer.py %s %s > %s' %(args.ri[i], args.q, args.Ri[i])
            cmds.append(cmd)
    
    # Submit commands and validate output
    ssub.submit_and_wait(cmds, args.z)
    ssub.validate_output(args.Fi + args.Ri, args.z)
    ssub.move_files(args.Fi + args.Ri, args.fi + args.ri, args.z)    


def merge_reads(args):
    # Merge forward and reverse reads using USEARCH
    
    # Intersect forward and reverse reads
    cmds = []
    for i in range(args.n):
        cmd = 'python ~/box/intersect_reads.py %s %s %s %s' %(args.fi[i], args.ri[i], args.Fi[i], args.Ri[i])
        cmds.append(cmd)
    ssub.submit_and_wait(cmds, args.z)
    ssub.validate_output(args.Fi + args.Ri, args.z)
    ssub.move_files(args.Fi + args.Ri, args.fi + args.ri, args.z)
    
    # Merge reads
    cmds = []
    for i in range(args.n):
        cmd = '%s -fastq_mergepairs %s -reverse %s -fastq_truncqual %d -fastqout %s' %(usearch, args.fi[i], args.ri[i], args.mi[i])
        cmds.append(cmd)
    ssub.submit_and_wait(cmds, args.z)
    ssub.validate_output(args.mi, args.z)
    ssub.remove_files(args.fi + args.ri, args.z)


def demultiplex(args):
    # Demultiplex samples using index and barcodes
    cmds = []
    for i in range(args.n):
        cmd = 'python ~/box/map_barcodes.py %s %s %s > %s' %(ci[i], args.b, args.x, Ci[i])
        cmds.append(cmd)
    ssub.submit_and_wait(cmds, args.z)
    ssub.validate_output(Ci, args.z)
    ssub.move_files(Ci, ci, args.z)


def quality_filter(args):
    # Quality filter with truncqual and maximum expected error
    cmds = []
    for i in range(args.n):
        cmd = '%s -fastq_filter %s -fastq_truncqual %d -fastq_maxee %f -fastaout %s' %(usearch, ci[i], args.truncqual, args.maxee, Ci[i])
        cmds.append(cmd)
    ssub.submit_and_wait(cmds, args.z)
    ssub.validate_output(Ci, args.z)
    ssub.move_files(Ci, ci, args.z)


def dereplicate(args):
    # Concatenate files and dereplicate
    cmd = 'cat %s > q.fst' %(' '.join(args.ci))
    ssub.run_local(cmd, args.z)
    ssub.validate_output(['q.fst'], args.z)
    cmd = 'rm %s' %(' '.join(args.ci))
    ssub.run_local(cmd, args.z)
    cmd = 'python ~/box/derep_fulllength.py q.fst q.derep.fst'
    ssub.submit_and_wait([cmd], args.z)
    ssub.validate_output(['q.derep.fst'], args.z)


def denovo_clustering(rename = True):
    
    # Denovo clustering with USEARCH
    cmds = []
    for i in range(len(args.sids)):
        sid = args.sids[i]
        cmd = '%s -cluster_otus q.derep.fst -otus %s -otuid .%d' %(usearch, args.oi[i], sid)
        cmds.append(cmd)
    ssub.submit_and_wait(cmds, args.z)
    ssub.validate_output(args.oi, args.z)
    
    # Rename OTUs
    if rename == True:
        cmds = []
        for i in range(len(args.sids)):
            sid = args.sids[i]
            cmd = 'python ~/bin/usearch_python/fasta_number.py %s OTU%d_ > %s' %(args.oi[i], sid, args.Oi[i])
            cmds.append(cmd)
        ssub.submit_and_wait(cmds, args.z)
        ssub.validate_output(args.Oi, args.z)
        ssub.move_files(args.Oi, args.oi, args.z)


def remove_chimeras(args):
    # Remove chimeras using gold database
    cmds = []
    for i in range(len(args.sids)):
        sid = args.sids[i]
        cmd = '%s -uchime_ref %s -db %s -nonchimeras %s -strand plus' %(usearch, args.oi[i], args.gold_db, args.Oi[i])
        cmds.append(cmd)
    ssub.submit_and_wait(cmds, args.z)
    ssub.validate_output(args.Oi, args.z)
    ssub.move_files(args.Oi, args.oi, args.z)


def reference_mapping(args):
    # Map reads to reference databases
    cmds = []
    for i in range(len(args.sids)):
        cmd = '%s -usearch_global q.derep.fst -db %s -uc %s -strand both -id .%d' %(usearch, args.db[i], args.uc[i], args.sids[i])
        cmds.append(cmd)
    ssub.submit_and_wait(cmds, args.z)
    ssub.validate_output(args.uc, args.z)


def make_otu_tables(args):
    # Make OTU tables from UC file
    cmds = []
    for i in range(len(sids)):
        cmd = 'python ~/usearch_python/uc2otutab.py %s %s' %(args.uc[i], args.xi[i])
        cmds.append(cmd)
    ssub.submit_and_wait(cmds, args.z)
    ssub.validate_output(args.xi, args.z)


def main():
    
    # Split fastq
    split_fastq(args)
    
    # Remove primers
    if args.primers == True:
        remove_primers(args)
    
    # Merge reads
    if args.merge == True:
        merge_reads(args)
        
    # Set current reads
    if args.merge == True:
        args.ci = args.mi
    else:
        args.ci = args.fi
    
    # Demultiplex
    if args.demultiplex == True:
        demultiplex(args)
    
    # Quality filter
    if args.qfilter == True:
        quality_filter(args)
    
    # Dereplicate reads
    dereplicate(args)

    # Denovo clustering
    if args.denovo == True:
        denovo_clustering(args, rename = True)
    
    # Map to reference database
    reference_mapping(args)
    
    # Make OTU tables
    make_otu_tables(args)


args = parse_args()

if __name__ == '__main__':
    main()