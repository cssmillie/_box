# Collapse a COTU table into OTUs of different lengths

import argparse
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('-i', help='input counts table', required=True)
parser.add_argument('-l', help='truncate length', required=True, type=int)
parser.add_argument('-o', help='output file', required=True)
args = parser.parse_args()

x = pd.read_table(args.i, sep = '\t', header = 0, index_col = 0)
m = {}

for v in x.columns:
    k = v[:args.l]
    m[k] = m.get(k, []) + [v]

y = {}
for k in m:
    y[k] = x.ix[:,m[k]].sum(axis=1)
y = pd.DataFrame(y)

y.to_csv(args.o, sep='\t')
