import argparse
import pandas as pd
import numpy as np
from util import *

def parse_args():
    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', default = '', help = 'input file')
    parser.add_argument('-o', default = '', help = 'output file')
    parser.add_argument('--row_regex', default = '', help = 'keep rows that match regex')
    parser.add_argument('--col_regex', default = '', help = 'keep cols that match regex')
    parser.add_argument('--transpose', default = False, action = 'store_true', help = 'transpose otu table')
    parser.add_argument('--pseudocount', default = np.nan, type = float, help = 'add pseudocount')
    parser.add_argument('--norm', default = False, action = 'store_true', help = 'normalize')
    parser.add_argument('--log', default = False, action = 'store_true', help = 'log transform')
    parser.add_argument('--locut', default = np.nan, type = float, help = 'minimum abundance (use with max_locut)')
    parser.add_argument('--hicut', default = np.nan, type = float, help = 'maximum abundance (use with max_hicut)')
    parser.add_argument('--max_locut', default = np.nan, type = float, help = 'remove otu if (fraction below locut) > max_locut')
    parser.add_argument('--max_hicut', default = np.nan, type = float, help = 'remove otu if (fraction above hicut) > max_hicut')
    parser.add_argument('--min_med', default = np.nan, type = float, help = 'min_med < median < max_med')
    parser.add_argument('--max_med', default = np.nan, type = float, help = 'min_med < median < max_med')
    parser.add_argument('--top', default = np.nan, type = float, help = 'select most abundant otus (fraction or int)')
    args = parser.parse_args()
    return args


def fmessage(data, text):
    message(text + ', shape = (%d, %d)' %(len(data.index), len(data.columns)))


def filter_otu_table(args, data):
    # filter otu table
    
    # filter by regex
    if args.row_regex:
        data = data.ix[[bool(re.search(args.row_regex, ri)) for ri in data.index], :]
    if args.col_regex:
        data = data.ix[:, [bool(re.search(args.col_regex, ci)) for ci in data.columns]]
    # transpose
    if args.transpose:
        data = data.transpose()
        fmessage('--transpose: transposing otu table')
    # add pseudocount
    if args.pseudocount:
        data = data + args.pseudocount
        fmessage('--pseudocount: adding %f to otu table' %(args.pseudocount))
    # normalize
    if args.norm:
        data = data.div(data.sum(axis=1), axis=0)
        fmessage('--norm: normalizing rows of otu table')
    # log transform
    if args.log:
        data = np.log(data)
        fmessage('--log: applying log transform')
    # filter by f <= locut
    if args.locut and args.max_locut:
        data = data.ix[:, (1.*(data <= args.locut).sum(axis=0) / len(data.index)) < args.max_locut]
        fmessage('--locut %f --max_locut %f: filtering by minimum abundance' %(args.locut, args.max_locut))
    # filter by f >= hicut
    if args.hicut and args.max_hicut:
        data = data.ix[:, (1.*(data >= args.hicut).sum(axis=0) / len(data.index)) < args.max_hicut]
        fmessage('--hicut %f --max_hicut %f: filtering by maximum abundance' %(args.hicut, args.max_hicut))
    # filter by median
    if args.min_med:
        data = data.ix[:, data.median(axis=0) >= args.min_med]
        fmessage('--min_med %f: filtering by median abundance' %(args.min_med))
    if args.max_med:
        data = data.ix[:, data.median(axis=0) <= args.max_med]
        fmessage('--max_med %f: filtering by maximum abundance' %(args.max_med))
    # select most abundant otus
    if args.top:
        if args.top < 1:
            data = data.ix[:, data.median(axis=0).order(ascending=False)[:int(args.top*len(data.index))].index]
            fmessage('--args.top %f: selecting top %f otus' %(args.top))
        elif args.top > 1:
            data = data.ix[:, data.median(axis=0).order(ascending=False)[:int(args.top)].index]
            fmessage('--args.top %d: selecting top %d otus' %(args.top))
    return data


def write_output(args, data):
    # write table as tab-delimited file
    data.to_csv(args.i, sep='\t')


args = parse_args()

if __name__ == '__main__':
    # load input as pandas dataframe
    data = read_dataframe(args.i)
    fmessage('loading %s as dataframe' %(args.i))
    data = filter_otu_table(args, data)
    write_output(data, args.o)