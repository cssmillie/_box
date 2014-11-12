import argparse, primer, sys, util
from string import maketrans

# Map FASTQ barcodes to samples
# input files:
#    1. FASTQ file (REQUIRED)
#    2. Barcodes map: tab-delimited map of sample ids -> barcodes (REQUIRED)
#    3. Index file: map of sequence ids -> barcodes (OPTIONAL)
# options:
# 


def parse_args():
    # Parse command line arguments and check for consistency
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', default='', help='fastq file', required=True)
    parser.add_argument('-b', default='', help='barcodes file (map of samples to barcodes)', required=True)
    parser.add_argument('--bfmt', default='fasta', help='barcodes file format', choices=['fasta', 'tab'])
    parser.add_argument('-i', default='', help='index file (map of sequence ids to barcodes)')
    parser.add_argument('--ifmt', default='fasta', help='index file format', choices=['fasta', 'tab'])
    parser.add_argument('-d', default=0, help='max number of barcode mismatches', type=int)
    parser.add_argument('--m1', help='mode 1: barcodes in sample ids', action='store_true', default=False)
    parser.add_argument('--m2', help='mode 2: barcodes in sequences', action='store_true', default=False)
    parser.add_argument('--m3', help='mode 3: barcodes in index file', action='store_true', default=False)
    parser.add_argument('--rc', help='reverse complement barcodes?', action='store_true', default=False)
    args = parser.parse_args()
    if True not in [args.m1, args.m2, args.m3]:
        quit('Error: must specify search mode --m1, --m2, or --m3')
    return args


rctab = maketrans('ACGTacgt','TGCAtgca')
def reverse_complement(x):
    # Reverse complement a sequence
    return x[::-1].translate(rctab)


def parse_barcodes_file(map_fn, format='fasta', rc=False):
    # Map barcodes to samples, taking the reverse complement if necessary
    b2s = {} # maps barcodes to samples
    # Case 1: barcodes file is FASTA format
    if format == 'fasta':
        for [sid, seq] in util.iter_fst(map_fn):
            if rc == True:
                seq = reverse_complement(seq)
            b2s[sid] = seq
    # Case 2: barcodes file is tab-delimited
    elif format == 'tab':
        for line in open(bcode_fn):
            # extract sample, barcode from mapping file
            s,b = line.rstrip().split()
            # reverse complement barcode (if necessary)
            if rc == True:
                b = reverse_complement(b)
            b2s[b] = s
    # Return map of barcodes to samples
    return b2s


def parse_index_file(index_fn, format='fasta'):
    # Map FASTQ sequences to their barcodes
    s2b = {} # maps sequences to barcodes
    # Case 1: index file is FASTA format
    if format=='fasta':
        for [sid, seq] in util.iter_fst(index_fn):
            s2b[sid] = seq
    # Case 2: index file is tab-delimited
    elif format=='tab':
        for line in open(index_fn):
            [sid, seq] = line.rstrip().split()
            s2b[sid] = seq
    return s2b


def extract_barcode_from_id(line):
    # for this type of fasta line:
    # @MISEQ:1:1101:14187:1716#ATAGGTGG/1
    bcode = line.split('#')[-1].split('/')[0]
    return bcode


def mismatches(seq, subseq, w):
    # Calculate the number of mismatches between a sequence and a given subsequence
    # Searches in a sliding window that starts at position 1 and ends at position w
    best_i = 0 # index (start position)
    best_d = len(sequence) # edit distance
    # for every start position
    for i in range(w):
        # calculate edit distance to the given subsequence
        d = primer.MatchPrefix(seq[i,:], subseq)
        # keep track of the best index and edit distance
        if d < best_d:
            best_i = i
            best_d = d
    return [best_i, best_d]


def find_best_match(seq, b2s, w, max_diff):
    # Find the sample with the best matching barcode to seq
    best_i = ''
    best_b = '' # barcode
    best_d = len(seq) # edit distance
    # calculate edit distance to every barcode
    for b in b2s:
        i, d = mismatches(seq, b, w) # get index, edit distance
        if d < best_d:
            best_i = i
            best_b = b
            best_d = d
    # return sample id of best match
    if best_d <= max_diff:
        return best_i, best_d, best_b, b2s[best_b]
    else:
        return ''


def run():
    # Maps FASTQ sequences to samples by finding the best matching barcodes
    # creates new sequence ids of the form: sample;count
    
    # initialize variables
    args = parse_args()
    b2s = parse_barcodes_file(args.b, format=args.bfmt, rc=args.rc) # barcodes to samples
    s2b = parse_index_file(args.i, format=args.ifmt) # samples to barcodes
    s2c = {} # samples to counts
    
    # For every FASTQ record...
    for record in util.iter_fsq(args.f):
        sid = record[0] # id
        seq = record[1] # sequence
        
        # Case 1: barcodes are in the sample IDs
        if args.m1:
            # extract barcode from the sequence id
            b = extract_barcode_from_id(line)
            # find sample with best matching barcode
            s = find_best_match(b, b2s, 1, args.d)[-1]
        
        # Case 2: barcodes are in the sequences
        elif args.m2:
            # search sequence for barcode
            i, d, b, s = find_best_match(seq, b2s, args.w, args.d)
            seq = seq[i+len(b):]
            
        
        # Case 3: barcodes are in index file
        elif args.m3:
            # get barcode from index file
            b = s2b[sid]
            # find sample with best matching barcode
            s = find_best_match(b, b2s, 1, args.d)[-1]
        
        # If sample, replace the current seqid with a new seqid
        if s:
            s2c = s2c.get(s, 0) + 1 # increment sample count
            new_sid = '@%s;%d' %(s, s2c)
            record[0] = new_sid
            print '\n'.join(record)

run()
