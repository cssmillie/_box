import argparse, primer, sys
from util import *


# Look for exact forward matches between primer and fasta sequences
# Not reverse complement!


def mismatches(seq, subseq, w):
    # calculate mismatches between seq and subseq with window size w
    best_i = 0
    best_d = len(seq)
    for i in range(w):
        d = primer.MatchPrefix(seq[i:], subseq) # edit distance
        if d < best_d:
            best_i = i
            best_d = d
    return [best_i, best_d]


def parse_args():
    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', default='', help='Input FASTA file')
    parser.add_argument('-q', default='', help='Input FASTQ file')
    parser.add_argument('-p', default='', help = 'Primer sequence')
    parser.add_argument('-d', default=0,  help = 'Max primer differences', type = int)
    args = parser.parse_args()
    return args


def remove_primers(fst_fn, primer, max_diffs):
    # remove primers
    PL = len(primer)
    k = 0
    for line in open(fst_fn):
        line = line.rstrip()
        k += 1
        if k%4 == 1:
            seqid = line[1:]
        elif k%4 == 2:
            seq = line
            I, D = mismatches(seq, primer, 15)
            if D > max_diffs:
                seqid = ''
            else:
                seq = seq[I+PL:]
        elif seqid != '' and k%4 == 0:
            quals = line[I+PL:]
            print '@%s\n%s\n+\n%s' %(seqid, seq, quals)


args = parse_args()
remove_primers(args.i, args.p, args.d)
