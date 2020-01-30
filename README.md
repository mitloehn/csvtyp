Reference Tables and How to Find Them

brute force: 

go through CSV corpus and check via subset, O(n^2)

use ray package for parallel processing

full run about 4 hours:

  make para

results:

tabs.tex for upload to shlat

changes in csv.db for further sql


csv.db:

not part of git repo as it is 2.2 GB

just create empty sqlite db

nerctools: 

cython version of subset check.  nice try, epic fail. slower than pure python.
