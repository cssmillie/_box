import argparse, util

parser = argparse.ArgumentParser()
parser.add_argument('--fst', default='input fasta file')
parser.add_argument('--otus', default='list of otus')
args = parser.parse_args()

otus = [line.rstrip() for line in open(args.otus)]

for [sid, seq] in util.iter_fst(args.fst):
    if sid in otus:
        print '>%s\n%s' %(sid, seq)
