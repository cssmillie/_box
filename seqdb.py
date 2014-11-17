from bidict import *
import os.path, os.system
import sys

class SeqDB():
	
	def __init__(self, fn):
		self.fn = fn
		self.db = {}
		self = self.load()
	
	
	def load(self):
		# Load db from file
		self.db = bidict({})
		print self.fn
		if os.path.exists(self.fn):
			for line in open(self.fn):
				[sid, seq] = line.rstrip().split()
				self.db[int(sid)] = seq
		return self
	
	
	def merge(self, fn, overwrite=False):
		# Merge with another db
		x = SeqDB(fn)
		if overwrite == True:
			self.db.update(x.db)
		else:
			total = len(self.db) + len(x.db)
			for sid in self.db:
				x.db[sid] = self.db[sid]
			if len(x.db) == total:
				self.db = x.db
			else:
				quit('error: overlap in db1 (%s) and db2 (%s), with overwrite=False')
		return self
	
	
	def otu2seq(self, otu):
		# If otu in db, get seq
		try:
			seq = self.db[otu]
			return seq
		except:
			quit('error: otu "%s "not in database' %(otu))
	
	
	def seq2otu(self, seq):
	    # If seq in db, get otu name
	    if seq in ~self.db:
	        otu = self.db[:seq]
	    # Otherwise, create new db entry
	    else:
	        if len(self.db) == 0:
	            otu = 1
	        else:
	            otu = max(self.db.keys()) + 1
	    self.db[:seq] = otu
	    # Return otu name
	    return otu
	
		
	def validate(self, fn):
		# Load file as SeqDB
		x = SeqDB(fn)
		# Test for equality
		if self.db == x.db:
			return True
		else:
			return False
	
	
	def write(self, out_fn=None):
	    if out_fn is None:
	        out_fn = self.fn
	    tmp_fn = '%s.tmp' %(out_fn)
	    out = open(tmp_fn, 'w')
	    for otu in self.db:
	        seq = self.db[otu]
	        out.write('%d\t%s\n' %(otu, seq))
	    out.close()
	    if self.validate(tmp_fn):
	        cmd = 'mv %s %s' %(tmp_fn, out_fn)
	        os.system(cmd)
	    return self
	
