# Reference Tables and How to Find Them

rayner.py

- Args: sample size (0 for all), list of filenames (e.g. obd.lst)

- named entity recognition with DBPedia and Wikidata gazetteer

- check for reference via subset, O(n^2)

- use ray package for parallel processing

- results are SQL statements in raydata.sql for input to Sqlite (see makefile)



tables.py

- run SQL queries on csv.db and create nice Latex tabular code

- file tabs.tex 

- each table is assigned a macro name starting with \tabs 


full run about 4 hours:

  make para


csv.db: 

not part of git repo, everything is created in rayner


nerctools: 

cython version of subset check.  nice try, epic fail. slower than pure python.
