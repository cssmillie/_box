import numpy as np
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--counts', required = True, help = 'otu counts table')
parser.add_argument('--min_count', required = True, help = 'min count per sample', type = int)
parser.add_argument('--min_samples', required = True, help = 'min number of samples', type = int)
parser.add_argument('--out', required = True, help = 'output file')
args = parser.parse_args()

h = []
x = []

for line in open(args.counts):
    line = line.rstrip().split('\t')
    if len(h) == 0:
        h = line
        x = np.zeros([len(h)-1])
        continue
    xi = (np.array([float(li) for li in line[1:]]) >= args.min_count)
    x += xi
    
x = (x >= args.min_samples)
x = np.insert(x, 0, True)

out = open(args.out, 'w')
for line in open(args.counts):
    line = np.array(line.rstrip().split('\t'))
    out.write('\t'.join(line[x])+'\n')
out.close()
