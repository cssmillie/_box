import argparse, re
import pandas as pd

# Get command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--fns', help='list of mapping files')
parser.add_argument('--ids', help='list of sample ids')
parser.add_argument('--out', help='outfile for otu table')
args = parser.parse_args()

# Count samples for each OTU
otus = {}

# Load filenames, file ids
fns = [line.rstrip() for line in open(args.fns)]
ids = [line.rstrip() for line in open(args.ids)]

# Get unique list of OTUs
for i,fn in enumerate(fns):
    for line in open(fn):
        line = line.rstrip().split()
        otu = line[0]
        otus[otu] = otus.get(otu, 0) + 1

# Initialize OTU table
rows = sorted(ids)
cols = sorted([otu for otu in otus if otus[otu] > 1])
data = pd.DataFrame(0, index=rows, columns=cols)

# Populate OTU table
for i,fn in enumerate(fns):
    sample = ids[i]
    for line in open(fn):
        line = line.rstrip().split()
        otu = line[0]
        count = len(line[1:])
        if otus[otu] > 1:
            data[otu][sample] = count

# Write OTU table
data.to_csv(args.out, index=True, header=True, sep='\t')
