import bz2
import re
import sys
import random
import os
import csv
import json
import gzip
import re
from collections import defaultdict, Counter
import pickle         
import numpy as np
if '--nerctools' in sys.argv:
  from nerctools import cntainb
if '--profile' in sys.argv:
  from line_profiler import LineProfiler
from time import time

fout = open('data.sql', 'w')

# TODO: most specific types; for now, remove Thing from type sets with more than one element
def refine(ts):
  res = []
  u = set()
  for s in ts:
    if s is None: continue
    if len(s) > 1: s = s.discard('www.w3.org/2002/07/owl#Thing')
    if s is None: continue
    res.append(s)
    for x in s: u.add(x)
  return res, u

def ignore(word):
  return all([ c in '1234567890.,' for c in word ])

def cleanfn(fn):
  return fn.replace("'", "SINGLEQUOTE").replace('"', 'DOUBLEQUOTE')

def readf(fn, ftyp):
  if ftyp == 'csv':
    rows = []
    try:
      f = open(fn)
      for row in csv.reader(f, delimiter=',', quoting=csv.QUOTE_ALL):
        rows.append(row)
        # if len(rows) > 5000: break
      f.close()
    except:
      err('tab', 'csv reader error', fn, -1)
      return -1
    if len(rows) == 0: 
      err('tab', 'no rows in table', fn, -1)
      return -1
    ncols = len(rows[0])
    h = 0
    try:
      if csv.Sniffer().has_header('\n'.join(str(v) for v in rows[:10])):
        h = 1
        rows = rows[1:]
        if len(rows) == 0: 
          err('tab', 'header only', fn, -1)
          return -1
    except:
      err('tab', 'csv sniffer error', fn, -1)
      return -1
    cols = list(map(list, zip(*rows)))
    return cols, h, len(rows), ncols
  #
  if ftyp == 'json':
    try:
      # webtables archive file contains thousands of tables, one per line
      lines = gzip.open(fn).readlines()
      while True:
        data = json.loads(random.choice(lines))
        if data['tableType'] == 'RELATION': break
      rel = data['relation']
      ncols = len(rel)
      h = data['hasHeader']
    except Exception as e: 
      print(e, file=sys.stderr)
      err('tab', 'json reader error', fn, -1)
      return -1
    cols = []
    for i in range(len(rel)):
      col = rel[i]
      if h: col = col[1:]
      nrows = len(col)
      cols.append(col)
    return cols, h, nrows, ncols

def err(typ, msg, fn, col):
    fout.write("insert into err (typ, msg, fn, col, src) values ('%s', '%s', '%s', %d, '%s');\n" % (typ, msg, cleanfn(fn), col, fn[6:9]))

# fraction of elements in a contained in b
def fracsubset(a, b):
  if '--nerctools' in sys.argv:
    return cntainb(a, b) / len(a)
  else: 
    return len(a.intersection(b)) / len(a)
  # return sum([ 1 for x in a if x in b ]) / len(a)
  # improve performance: try with small subset of a first
  # n = 20
  # if len(a) <= n:
  #   return sum([ 1 for x in a if x in b ]) / len(a)
  # f = sum([ 1 for x in random.sample(a, n) if x in b ]) / n
  # if f < lim: 
  #   return f
  # return sum([ 1 for x in a if x in b ]) / len(a)

# find case of elements in string set: U all upper, L all lower, M mixed
def getcase(st):
  if all([ x.isupper() for x in st ]): return 'U'
  if all([ x.islower() for x in st ]): return 'L'
  return 'M'

# find sets all alpha or all not alpha
def getalph(st):
  if all([ x.isalpha() for x in st ]): return 'A'
  if all([ not x.isalpha() for x in st ]): return 'N'
  return 'M'

def main():
  ss = int(sys.argv[1])
  filelist = sys.argv[2]
  GB = 1024*1024*1024
  t0 = time()
  if '--profile' in sys.argv or '--timing' in sys.argv:
    types, labels = defaultdict(set), defaultdict(set) # save time for profiling test
  else:
    print('reading typeslabels.pkl ..', file=sys.stderr)
    f = open('typeslabels.pkl', 'rb')
    types, labels = pickle.load(f)
    f.close()  
  t1 = time()
  print('labels in dbpedia and wikidata:', len(types), file=sys.stderr)
  print('labels read in %.1f seconds or %.1f minutes' % (t1-t0, (t1-t0)/60), file=sys.stderr)
  # sys.getsizeof does not work correctly, and pympler is WAY too slow
  # print('sizeof types %.3f GB  sizeof labels %.3f GB' % (sys.getsizeof(types)/GB, sys.getsizeof(labels)/GB,), file=sys.stderr)
  files = [ f.strip() for f in open(filelist).readlines() ]
  if ss == 0: ss = len(files)
  nrows = np.zeros(ss)
  ncols = np.zeros(ss)
  cols = {}
  svals = 0
  ne = 0
  rels = {}
  titles = {}
  typcnt = defaultdict(int)
  usecnt = defaultdict(int)
  typsets = {}
  tabs = {}
  nval = {}
  stats = {}
  hdr = 0
  rtyp = defaultdict(int) # consistent row types
  # pat = re.compile("^[0-9\.,/Q_-]*$")
  print('reading', ss, 'out of total', len(files), 'files from', filelist, file=sys.stderr)
  random.seed(4713)
  sam = random.sample(files, ss)
  fout.write("begin transaction;\n")
  fout.write('DROP TABLE IF EXISTS tab;\n')
  fout.write('DROP TABLE IF EXISTS col;\n')
  fout.write('DROP TABLE IF EXISTS err;\n')
  fout.write('DROP TABLE IF EXISTS sel;\n')
  fout.write('DROP TABLE IF EXISTS sub;\n')
  fout.write('CREATE TABLE tab (id int, rows int, cols int, head int, fn varchar, src varchar);\n')
  fout.write('CREATE TABLE col (tab int, col int, typ varchar(255), frac float, cov float);\n')
  fout.write('CREATE TABLE err (typ varchar, msg varchar, fn varchar, col int, src varchar);\n')
  fout.write('CREATE TABLE sel (tab int, col int, nval int, ndist int, sel int);\n')
  fout.write('CREATE TABLE sub (l int, j int, k int, i int, comtyp int, fracsubset float, nchild int, nparent int, fnchild varchar, fnparent varchar);\n')
  for k in range(ss):
    fn = sam[k]
    src = fn[6:9]
    if src in ['kag', 'obd']: ftyp = 'csv'
    else: ftyp = 'json'
    res = readf(fn, ftyp)
    if res == -1: continue
    cols, h, nrows, ncols = res
    #
    # kaggle filenames contain ' !!!
    fout.write("insert into tab values (%d, %d, %d, %d, '%s', '%s');\n" % (k, nrows, ncols, h, cleanfn(fn), src));
    nval[k] = {} # lengths of columns as lists, after removing ignored elements
    tabs[k] = {} # sets of col values, after removing ignored
    typsets[k] = {}
    stats[k] = {}
    for i in range(len(cols)):
      lst = [ x.strip() for x in cols[i] if len(x.strip()) > 0 and not ignore(x.strip()) and x.strip().lower() not in ['null', 'none'] ]
      if len(lst) == 0:
        err('col', 'all num or null', fn, i)
        continue
      # only need list length and set for finding reference cols, not whole list: save memory
      nval[k][i] = len(lst) 
      tabs[k][i] = set(lst) 
      # selectivity 
      slst = set(lst)
      sel = len(slst) / len(lst)
      fout.write("insert into sel (tab, col, nval, ndist, sel) values(%d, %d, %d, %d, %f);\n" % (k, i, len(lst), len(slst), sel))
      # None types?
      tsets = [ types[x] for x in lst ]
      ts = set.union(*tsets)
      typsets[k][i] = set()
      if '--stats' in sys.argv:
        stats[k][i] = {}
        lens = [ len(x) for x in slst ]
        stats[k][i]['lmin'] = min(lens)
        stats[k][i]['lmax'] = max(lens)
        stats[k][i]['case'] = getcase(slst)
        stats[k][i]['alph'] = getalph(slst)
      for t in ts:
        # for more than one known value of type t: fraction of column values that are of type t
        if len(labels[t]) < 2: continue
        f = sum([ int(t in s) for s in tsets ]) / len(lst)
        # type coverage: fraction of col values of type t in relation to all known values of this type
        tset = set([ x for x in lst if t in types[x] ]) # col vals of type t
        cov = len(tset) / len(labels[t]) 
        fout.write("insert into col (tab, col, typ, frac, cov) values (%d, %d, '%s', %f, %f);\n" % 
          (k, i, t.replace("'", "").replace('"', ''), f, cov))
        if f >= 0.8:
          typsets[k][i].add(t)
      typsets[k][i].discard('www.w3.org/2002/07/owl#Thing') # still necessary ?
  t2 = time()
  print('files read in %.1f seconds or %.1f minutes' % (t2-t1, (t2-t1)/60), file=sys.stderr)
  # print('sizeof tabs %.3f GB  sizeof typsets %.3f GB' % (sys.getsizeof(tabs)/GB, sys.getsizeof(typsets)/GB,), file=sys.stderr)
  #
  print("finding reference columns..", file=sys.stderr)
  for k in tabs.keys():
    # cannot use range() here: some i can be missing !!
    for i in tabs[k].keys(): 
      x = tabs[k][i] # set!
      # check conditions for reference column
      # minimum 10 distinct values
      if len(x) < 10: continue
      # selectivity == 1
      if len(x) != nval[k][i]: continue
      for l in tabs.keys():
        if l == k: continue
        for j in tabs[l].keys():
          # check stats, maybe save some work?
          if '--stats' in sys.argv:
            if stats[l][j]['lmax'] < stats[k][i]['lmin']: continue #  all shorter
            if stats[l][j]['lmin'] > stats[k][i]['lmax']: continue #  all longer
            if stats[l][j]['case'] == 'U' and stats[k][i]['case'] == 'L': continue 
            if stats[l][j]['case'] == 'L' and stats[k][i]['case'] == 'U': continue 
            if stats[l][j]['alph'] == 'A' and stats[k][i]['alph'] == 'N': continue 
            if stats[l][j]['alph'] == 'N' and stats[k][i]['alph'] == 'A': continue 
          y = tabs[l][j]
          if len(y) < 3: continue # avoid boolean
          # common types
          comtyp = typsets[k][i] & typsets[l][j]
          # if len(comtyp) == 0: continue # not good, miss most refs
          # if y <= x: # (strict) subset # instead: 90% subset
          fracsubs = fracsubset(y, x)
          if fracsubs >= 0.9:
            # table l, column j is subset of table k, column i: table l references table k ?
            fout.write("insert into sub (l, j, k, i, comtyp, fracsubset, nchild, nparent, fnchild, fnparent)"
              + " values (%d, %d, %d, %d, '%s', %f, %d, %d, '%s', '%s');\n" 
              % (l, j, k, i, len(comtyp), fracsubs, len(y), len(x), cleanfn(sam[l]), cleanfn(sam[k])))
  fout.write("commit;\n")
  fout.close()
  t3 = time()
  print('references done in %.1f seconds' % (t3-t2,), file=sys.stderr)
  print('total run time %.1f seconds or %.1f minutes' % (t3-t0, (t3-t0)/60), file=sys.stderr)
  sys.exit(0)

if '--profile' in sys.argv:
  print('profiling..', file=sys.stderr)
  prof = LineProfiler(main)
  prof.run('main()')
  prof.print_stats()
else:
  main()

