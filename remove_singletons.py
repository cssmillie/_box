import sys, argparse, util

parser = argparse.ArgumentParser()
parser.add_argument('--fst', help='fasta infile')
parser.add_argument('--map', help='map infile')
parser.add_argument('--out', help='fasta outfile')
args = parser.parse_args()

def is_singleton(line):
    line = line.rstrip().split()
    if len(line) > 2:
        return False
    elif int(line[1].split(':')[-1]) > 1:
        return False
    else:
        return True

keep = []
for line in open(args.map):
    if is_singleton(line):
        continue
    else:
        keep.append(line.rstrip().split()[0])

out = open(args.out, 'w')
for [sid, seq] in util.iter_fst(args.fst):
    if sid in keep:
        out.write('>%s\n%s\n' %(sid, seq))
out.close()
