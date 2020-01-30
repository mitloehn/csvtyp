# Reference Tables and How to Find Them

rayner.py

- named entity recognition with DBPedia and Wikidata gazetteer

- check for reference via subset, O(n^2)

- use ray package for parallel processing

tables.py

- run SQL queries on csv.db and create nice Latex tabular code

- each table is assigned a macro name starting with \tabs 


full run about 4 hours:

  make para

results:

- tabs.tex for upload to shlat

- changes in csv.db for further sql


csv.db: not part of git repo, everything is created in rayner


nerctools: 

cython version of subset check.  nice try, epic fail. slower than pure python.
