import argparse
import primer
import sys
from string import maketrans



# Map barcodes to samples
# Inputs:
# 1. Fasta file
# 2. Barcode map (sample -> barcode)
# 3. Index file (optional)
# **May need to edit the extract_barcodes function



rctab = maketrans('ACGTacgt','TGCAtgca')

def reverse_complement(x):
    return x[::-1].translate(rctab)

def mismatches(seq, subseq, w):
    I = 0
    D = len(seq)
    for i in range(w):
        d = primer.MatchPrefix(seq[i:], subseq)
        if d < D:
            I = i
            D = d
    return [I, D]

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', default = '', help = 'fasta file')
    parser.add_argument('-b', default = '', help = 'barcodes map (samples -> barcodes)')
    parser.add_argument('-i', default = '', help = 'index file')
    parser.add_argument('-d', default = 0,  help = 'max barcode mismatches', type = int)
    parser.add_argument('--rc', default = False, action = 'store_true', help = 'reverse complement?')
    args = parser.parse_args()
    return args

def map_barcodes(bcode_fn, rc = False):
    b2s = {}
    for line in open(bcode_fn):
        s, b = line.rstrip().split()
        if rc == True:
            b = reverse_complement(b)
        b2s[b] = s
    return b2s

def extract_barcode(line):
    # for this type of fasta line:
    # @MISEQ:1:1101:14187:1716#ATAGGTGG/1
    bcode = line.split('#')[-1].split('/')[0]
    return bcode

def find_best_match(bcode, b2s, max_diff):
    B = '' # best barcode
    D = '' # differences
    for b in b2s:
        q, d = mismatches(bcode, b, 1)
        if d < D:
            B = b
            D = d
    if D <= max_diff:
        return b2s[B]
    else:
        return ''

def map_fasta(fst_fn, b2s, max_diff):
    s2c = {}
    k = 0
    for line in open(fst_fn):
        line = line.rstrip()
        k += 1
        if k%4 == 1:
            b = extract_barcode(line)
            s = find_best_match(b, b2s, max_diff)
            if s:
                s2c[s] = s2c.get(s, 0) + 1
                seqid = '@%s;%d' %(s, s2c[s])
                line = seqid
            else:
                seqid = ''
        if seqid:
            print line

args = parse_args()
b2s = map_barcodes(args.b, rc = args.rc)
map_fasta(args.f, b2s, args.d)
