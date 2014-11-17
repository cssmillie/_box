from bidict import *
import os.path, os
import sys

class SeqDB():
	
	def __init__(self, fn):
		self.fn = fn
		self = self.load_db()
	
	
	def load_db(self):
		# Initialize empty SeqDB
		self.db = bidict({})
		# Load existing SeqDB (if exists)
		if os.path.exists(self.fn):
			for line in open(self.fn):
				[sid, seq] = line.rstrip().split()
				self.db[int(sid)] = seq
		return self
	
	
	def add_seq(self, seq):
		# Add new sequence to SeqDB
		if seq not in ~self.db:
			# If SeqDB is empty, set OTU = 1
			if len(self.db) == 0:
				otu = 1
			# Otherwise, increment to get next OTU
			else:
				otu = max(self.db) + 1
			self.db[:seq] = otu
		return otu
	
	
	def merge_db(self, x, keep=0):
		# Merge SeqDB with another database
		# If keep == 0, use ids in self
		if keep == 0:
			for seq in ~x.db:
				self.add_seq(seq)
			return self
		# If keep == 0, use ids in x
		elif keep == 1:
			for seq in ~self.db:
				x.add_seq(seq)
			return x
	
	
	def get_seq(self, otu):
		# Get sequence associated with OTU id
		try:
			seq = self.db[otu]
			return seq
		except:
			quit('error: otu "%s "not in database' %(otu))
	
	
	def get_otu(self, seq):
		# Get OTU id associated with sequence
		# If sequence in SeqDB, get OTU id
		if seq in ~self.db:
			otu = self.db[:seq]
		# Otherwise, create new SeqDB entry
		else:
			otu = self.add_seq(seq)
		# Return OTU id
		return otu
	
		
	def trim_db(self, l, keep_all=False):
		# Trim sequences in SeqDB to length l
		for seq in ~self.db:
			new_seq = seq[:l]
			self.add_seq(new_seq)
			# Remove other sequences from SeqDB (unless keep_all==True)
			if keep_all == False:
				if len(seq) != l:
					del self.db[:seq]
		return self
	
	
	def validate(self, fn):
		# Load file as SeqDB
		x = SeqDB(fn)
		# Test for equality
		if self.db == x.db:
			return True
		else:
			return False
	
	
	def write(self, out_fn=None, overwrite=False):
		
		# If no outfile, use infile
		if out_fn is None:
			out_fn = self.fn
			while overwrite == False and os.path.exists(out_fn):
				out_fn += '.bk'
		
		# Write SeqDB to tempfile
		tmp_fn = '%s.tmp' %(out_fn)
		tmp = open(tmp_fn, 'w')
		for otu in self.db:
			seq = self.db[otu]
			tmp.write('%d\t%s\n' %(otu, seq))
		tmp.close()
		
		# Validate and move to final destination
		if self.validate(tmp_fn):
			cmd = 'mv %s %s' %(tmp_fn, out_fn)
			os.system(cmd)
		return self
	
	
	def map_db(self, x, reverse=False):
		# Map OTUs in self to OTUs in another SeqDB
		if reverse == False:
			m = {}
			for seq in ~self.db:
				otu1 = self.get_otu(seq)
				otu2 = x.get_otu(seq)
				m[otu1] = otu2
		# Or map OTUs in another SeqDB to self
		elif reverse == True:
			m = {}
			for seq in ~self.x:
				otu1 = x.get_otu(seq)
				otu2 = self.get_otu(seq)
				m[otu1] = otu2
		# Otherwise, throw error
		else:
			quit('error: invalid argument in map_to_db()')
		return m
	
