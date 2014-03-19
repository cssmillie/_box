import argparse, ssub, os
from util import *
ssub = ssub.Ssub()

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
    if args.all == True:
        args.primers = args.merge = args.demultiplex = args.qfilter = args.chimeras = args.ref_gg = True
    args.sids = map(int, args.sids.split(','))
        
    return args


class OTU_Caller():
    
    def __init__(self):
        # initialize variables
        self.usearch = 'usearch'
        self.ggdb = '~/db/gg_otus_4feb2011'
        # copy command line arguments
        self.__dict__.update(parse_args().__dict__)
        # create filenames
        self.get_filenames()
    
    
    def get_filenames(self):
        
        # Generate filenames to use in pipeline
        f_base = os.path.basename(self.f)
        r_base = os.path.basename(self.r)
        self.fi = ['%s.%d' %(self.f, i) for i in range(self.n)] # forward reads (split)
        self.ri = ['%s.%d' %(self.r, i) for i in range(self.n)] # reverse reads (split)
        self.mi = ['%s.%d.merge' %(self.f, i) for i in range(self.n)] # merged reads (split)
        self.Fi = ['%s.%d.tmp' %(self.f, i) for i in range(self.n)] # forward reads (temp)
        self.Ri = ['%s.%d.tmp' %(self.r, i) for i in range(self.n)] # reverse reads (temp)
        self.Mi = ['%s.%d.tmp' %(self.f, i) for i in range(self.n)] # merged reads (temp)
        self.ci = ['q.%d.fst' %(i) for i in range(self.n)] # current reads
        self.Ci = ['q.%d.tmp' %(i) for i in range(self.n)] # current reads (temp)
        self.oi = ['otus.%d.fst' %(sid) for sid in self.sids] # otu representative sequences
        self.Oi = ['otus.%d.tmp' %(sid) for sid in self.sids] # otu representative sequences (temp)
        self.uc = ['otus.%d.uc' %(sid) for sid in self.sids] # uclust output files
        self.xi = ['otus.%d.counts' %(sid) for sid in self.sids] # otu tables (counts)
        
        # Get database for read mapping
        if self.denovo == True:
            self.db = self.oi
        elif self.ref_gg == True:
            self.db = ['%s/rep_set/gg_%d_otus_4feb2011.fasta' %(self.ggdb, sid) for sid in self.sids]
    
    
    def split_fastq(self):
        # Split forward and reverse reads (for parallel processing)
        
        # Get list of commands
        cmds = []
        if self.f:
            cmd = 'python ~/box/split_fastq.py %s %s' %(self.f, self.n)
            cmds.append(cmd)
        if self.r:
            cmd = 'python ~/box/split_fastq.py %s %s' %(self.r, self.n)
            cmds.append(cmd)
        
        # Submit commands and validate output
        ssub.submit_and_wait(cmds, out = self.z)
        ssub.validate_output(self.fi + self.ri, out = self.z)
    
    
    def remove_primers(self):
        # Remove diversity region + primer and discard reads with > 2 mismatches
        
        # Get list of commands
        cmds = []
        for i in range(self.n):
            if self.f:
                cmd = '~/box/remove_primer.py %s %s > %s' %(self.fi[i], self.p, self.Fi[i])
                cmds.append(cmd)
            if self.r:
                cmd = '~/box/remove_primer.py %s %s > %s' %(self.ri[i], self.q, self.Ri[i])
                cmds.append(cmd)
        
        # Submit commands and validate output
        ssub.submit_and_wait(cmds, out = self.z)
        ssub.validate_output(self.Fi + self.Ri, out = self.z)
        ssub.move_files(self.Fi + self.Ri, self.fi + self.ri, out = self.z)
    
    
    def merge_reads(self):
        # Merge forward and reverse reads using USEARCH
        
        # Intersect forward and reverse reads
        cmds = []
        for i in range(self.n):
            cmd = 'python ~/box/intersect_reads.py %s %s %s %s' %(self.fi[i], self.ri[i], self.Fi[i], self.Ri[i])
            cmds.append(cmd)
        ssub.submit_and_wait(cmds, out = self.z)
        ssub.validate_output(self.Fi + self.Ri, out = self.z)
        ssub.move_files(self.Fi + self.Ri, self.fi + self.ri, out = self.z)
        
        # Merge reads
        cmds = []
        for i in range(self.n):
            cmd = '%s -fastq_mergepairs %s -reverse %s -fastq_truncqual %d -fastqout %s' %(self.usearch, self.fi[i], self.ri[i], self.truncqual, self.mi[i])
            cmds.append(cmd)
        ssub.submit_and_wait(cmds, out = self.z)
        ssub.validate_output(self.mi, out = self.z)
        ssub.remove_files(self.fi + self.ri, out = self.z)
    
    
    def demultiplex_reads(self):
        # Demultiplex samples using index and barcodes
        cmds = []
        for i in range(self.n):
            cmd = 'python ~/box/map_barcodes.py %s %s %s > %s' %(self.ci[i], self.b, self.x, self.Ci[i])
            cmds.append(cmd)
        ssub.submit_and_wait(cmds, self.z)
        ssub.validate_output(self.Ci, self.z)
        ssub.move_files(self.Ci, self.ci, self.z)
    
    
    def quality_filter(self):
        # Quality filter with truncqual and maximum expected error
        cmds = []
        for i in range(self.n):
            cmd = '%s -fastq_filter %s -fastq_truncqual %d -fastq_maxee %f -fastaout %s' %(self.usearch, self.ci[i], self.truncqual, self.maxee, self.Ci[i])
            cmds.append(cmd)
        ssub.submit_and_wait(cmds, self.z)
        ssub.validate_output(self.Ci, self.z)
        ssub.move_files(self.Ci, self.ci, self.z)
    
    
    def dereplicate(self):
        # Concatenate files and dereplicate
        cmd = 'cat %s > q.fst' %(' '.join(self.ci))
        ssub.run_local([cmd], out = self.z)
        ssub.validate_output(['q.fst'], self.z)
        cmd = 'rm %s' %(' '.join(self.ci))
        ssub.run_local([cmd], out = self.z)
        cmd = 'python ~/box/derep_fulllength.py q.fst q.derep.fst'
        ssub.submit_and_wait([cmd], self.z)
        ssub.validate_output(['q.derep.fst'], self.z)
    
    
    def denovo_clustering(rename = True):
        
        # Denovo clustering with USEARCH
        cmds = []
        for i in range(len(self.sids)):
            sid = self.sids[i]
            cmd = '%s -cluster_otus q.derep.fst -otus %s -otuid .%d' %(self.usearch, self.oi[i], sid)
            cmds.append(cmd)
        ssub.submit_and_wait(cmds, self.z)
        ssub.validate_output(self.oi, self.z)
        
        # Rename OTUs
        if rename == True:
            cmds = []
            for i in range(len(self.sids)):
                sid = self.sids[i]
                cmd = 'python ~/bin/usearch_python/fasta_number.py %s OTU%d_ > %s' %(self.oi[i], sid, self.Oi[i])
                cmds.append(cmd)
            ssub.submit_and_wait(cmds, self.z)
            ssub.validate_output(self.Oi, self.z)
            ssub.move_files(self.Oi, self.oi, self.z)
    
    
    def remove_chimeras(self):
        # Remove chimeras using gold database
        cmds = []
        for i in range(len(self.sids)):
            sid = self.sids[i]
            cmd = '%s -uchime_ref %s -db %s -nonchimeras %s -strand plus' %(self.usearch, self.oi[i], self.gold_db, self.Oi[i])
            cmds.append(cmd)
        ssub.submit_and_wait(cmds, self.z)
        ssub.validate_output(self.Oi, self.z)
        ssub.move_files(self.Oi, self.oi, self.z)
    
    
    def reference_mapping(self):
        # Map reads to reference databases
        cmds = []
        for i in range(len(self.sids)):
            cmd = '%s -usearch_global q.derep.fst -db %s -uc %s -strand both -id .%d' %(self.usearch, self.db[i], self.uc[i], self.sids[i])
            cmds.append(cmd)
        ssub.submit_and_wait(cmds, self.z)
        ssub.validate_output(self.uc, self.z)
    
    
    def make_otu_tables(self):
        # Make OTU tables from UC file
        cmds = []
        for i in range(len(self.sids)):
            cmd = 'python ~/usearch_python/uc2otutab.py %s %s' %(self.uc[i], self.xi[i])
            cmds.append(cmd)
        ssub.submit_and_wait(cmds, self.z)
        ssub.validate_output(self.xi, self.z)
    


def main():
    
    # Initialize OTU caller
    oc = OTU_Caller()
    
    # Split fastq
    message('Splitting fastq')
    oc.split_fastq()
    
    # Remove primers
    if oc.primers == True:
        message('Removing primers')
        oc.remove_primers()
    
    # Merge reads
    if oc.merge == True:
        message('Merging reads')
        oc.merge_reads()
        
    # Set current reads
    if oc.merge == True:
        oc.ci = oc.mi
    else:
        oc.ci = oc.fi
    
    # Demultiplex
    if oc.demultiplex == True:
        message('Demultiplexing')
        oc.demultiplex_reads()
    
    # Quality filter
    if oc.qfilter == True:
        message('Quality filtering')
        oc.quality_filter()
    
    # Dereplicate reads
    message('Dereplicating sequences')
    oc.dereplicate()
    
    # Denovo clustering
    if oc.denovo == True:
        message('Denovo clustering')
        oc.denovo_clustering(rename = True)
    
    # Map to reference database
    message('Mapping to reference')
    oc.reference_mapping()
    
    # Make OTU tables
    message('Making OTU tables')
    oc.make_otu_tables()


if __name__ == '__main__':
    main()
