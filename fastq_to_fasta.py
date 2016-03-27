#!/usr/bin/env python
import util
q = util.iter_fsq()
for record in q:
    print '>%s\n%s' %(record[0][1:], record[1])
