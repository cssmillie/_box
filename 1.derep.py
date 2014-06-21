# Dereplicate sequences in fasta file

import util, argparse, re

def parse_args():
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', help='Input FASTA file', required=True)
    parser.add_argument('-o', help='Output FASTA file', required=True)
    parser.add_argument('-m', help='Output mapping file', required=True)
    parser.add_argument('-s', help='Sample ID separator', required=True)
    args = parser.parse_args()
    return args


def dereplicate(fn):
    # Dereplicate sequences
    x = {}
    for [sid, seq] in util.iter_fst(fn):
        sa = re.search('(.*?)%s' %(args.s), sid).group(1)
        if seq not in x:
            x[seq] = {}
        if sa not in x[seq]:
            x[seq][sa] = 0
        x[seq][sa] += 1
    return x


def write_output(x, ofn, mfn):
    # Write output (dereplicated fasta file + mapping file)
    fst_out = open(ofn, 'w')
    map_out = open(mfn, 'w')
    otu_id = 0
    for seq in x:
        otu_id += 1
        fst_out.write('>%s;size=%d;\n%s\n' %(otu_id, sum(x[seq].values()), seq))
        map_out.write('%s;size=%d;\t%s\n' %(otu_id, sum(x[seq].values()), ' '.join(['%s:%d' %(sa, x[seq][sa]) for sa in x[seq]])))
    fst_out.close()
    map_out.close()


args = parse_args()
x = dereplicate(args.i)
write_output(x, args.o, args.m)
