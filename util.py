import re, string, sys, time

rctab = string.maketrans('ACGTacgt','TGCAtgca')

def message(text, indent=2):
    # print message to stderr
    space = ' ' * indent
    text = re.sub('\n', '\n%s' %(space), text)
    sys.stderr.write('%s%s\n' %(space, text))


def error(text, indent=2):
    # print message to stderr and quit
    space = ' ' * indent
    text = re.sub('\n', '\n%s' %(space), text)
    sys.stderr.write('%s%s\n' %(space, text))
    quit()


def read_list(fn, dtype=str):
    # read file as list
    x = [dtype(line.rstrip()) for line in open(fn)]
    return x


def read_dataframe(fn, index_dtype=str, columns_dtype=str):
    import pandas as pd
    # read file as pandas dataframe
    x = pd.read_table(fn, sep='\t', header=0, index_col=0)
    #x.index = x.index.astype(index_dtype)
    #x.columns = x.columns.astype(columns_dtype)
    return x


def read_tseries(fn):
    import pandas as pd
    # read file as pandas time series
    return read_dataframe(fn, index_dtype=float, columns_dtype=str)


def iter_fst(fn):
    # generator that iterates through [sid, seq] pairs in a fasta file
    seq = ''
    for line in open(fn):
        line = line.rstrip()
        if line.startswith('>'):
            if seq != '':
                yield [sid, seq]
            sid = line
            seq = ''
        else:
            seq += line
    yield [sid, seq]


def iter_fsq(fn):
    # generator that iterates through records in a fastq file
    record = []
    i = 0
    for line in open(fn):
        i += 1
        if i % 4 == 1:
            if len(record) > 0:
                yield record
            record = []
        record.append(line.rstrip())
    yield record


def read_fst(fn, reverse=False):
    # read fasta file as dictionary
    fst = {}
    for [sid, seq] in iter_fst(fst):
        if reverse == False:
            fst[sid] = seq
        elif reverse == True:
            fst[seq] = sid
    return fst


def cycle(x):
    # an efficient way to cycle through a list (similar to itertools.cycle)
    while True:
        for xi in x:
            yield xi


class timer():
    # generator that measures elapsed time
    def __init__(self):
        self.t = [time.time()]
    def __iter__(self):
        return self
    def next(self):
        self.t.append(time.time())
        return self.t[-1] - self.t[-2]
    def mean(self):
        return np.mean(self.t)
    def median(self):
        return np.median(self.t)

def reverse_complement(x):
    return x[::-1].translate(rctab)

def pretty_colors(format='hex'):
    
    def rgb_to_hex(rgb):
        return '#%02x%02x%02x' % rgb

    colors = [(31,  119, 180), (174, 199, 232), (255, 127, 14),  (255, 187, 120),  
              (44,  160, 44),  (152, 223, 138), (214, 39,  40),  (255, 152, 150),  
              (148, 103, 189), (197, 176, 213), (140, 86,  75),  (196, 156, 148),  
              (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),  
              (188, 189, 34),  (219, 219, 141), (23,  190, 207), (158, 218, 229)]  
    
    if format == 'rgb':
        return colors

    if format == 'hex':
        return [rgb_to_hex(color) for color in colors]

    if format == 'dec':
        for i in range(len(colors)):  
            r, g, b = colors[i]  
            colors[i] = (r / 255., g / 255., b / 255.)
        return colors

