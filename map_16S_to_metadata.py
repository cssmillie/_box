import argparse, ssub

def parse_args():
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', 'input otu representative sequences')
    parser.add_argument('-d', 'ncbi 16S database', default='/net/radiodurans/alm/lab/db/ncbi/finished/all.16S.fst')
    parser.add_argument('-c', 'percent identity cutoff', type=float)
    parser.add_argument('-n', 'number of nodes to use', type=int)
    parser.add_argument('-o', 'output mapping')
    args = parser.parse_args()
    return args

def map_to_ncbi(args, fn):
    # Given fasta file, map sequences to NCBI
    
    # First, split fasta into [n] files
    cmd = 'python ~/box/split_fasta.py -i %s -n %d' %(args.i, args.n)
    ssub.submit_and_wait(cmd)
    
    # Get split fasta filenames
    fns = ['%s.%d' %(fn, i) for i in range(args.n)]

    # Map to database
    cmds = ['usearch %s' %(fn) for fn in fns]
    ssub.submit_and_wait(cmds)

    # Concatenate the output
    cmd = ''

    # Map to metadata
