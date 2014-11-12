import argparse, primer, sys
from util import *


# Look for exact forward matches between primer and fasta sequences
# Not reverse complement!


def mismatches(seq, subseq, w):
    # calculate mismatches between seq and subseq with window size w
    I = 0
    D = len(seq)
    for i in range(w):
        d = primer.MatchPrefix(seq[i:], subseq)
        if d < D:
            I = i
            D = d
    return [I, D]


def parse_args():
    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', default = '', help = 'input fasta file')
    parser.add_argument('-p', default = '', help = 'primer sequence')
    parser.add_argument('-d', default = 0,  help = 'max primer differences', type = int)
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