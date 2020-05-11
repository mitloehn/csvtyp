import sqlite3
conn = sqlite3.connect('csv.db')

# print('\\begin{center}\\begin{tabular}{lrrr}')
# print('Source & Number of Tables & Avg Rows & Avg Columns \\\\ \\hline')
# for row in c.execute('''
#   select 
#     src as Source, 
#     count(*) as Tables, 
#     round(avg(rows),1) as "Avg Rows", 
#     round(avg(cols),1) as "Avg Columns" 
#   from tab group by Source'''):
#   print(' & '.join([ str(x) for x in row]) + ' \\\\')
# print('\\end{tabular}\\end{center}')

def repl(s):
  return s.replace('#', '\\#').replace('*', '$\\star$')

f = open('tabs.tex', 'w')

def table(fn, head, align, sql):
  f.write('\n'.join([ '% ' + x for x in sql.split('\n') ]) + '\n')
  f.write('\\newcommand{\\' + fn +'}{')
  f.write('\\begin{center}\n\\begin{tabular}{' + align + '}\n')
  f.write(head + '\\\\ \\hline\n')
  c = conn.cursor()
  for row in c.execute(sql):
    f.write(' & '.join([ repl(str(x)) for x in row]) + ' \\\\\n')
  f.write('\\end{tabular}\n\\end{center}\n}\n')

# values
table('tabsvals', ' & Total & With Type & Percent', 'lrrr', '''
  select desc, nvals, wtyp, round(frac, 3) from val''')

# number of tables read from files, with rows and cols, before processing
table('tabsbefore', 'Tables & Header & Columns & Avg Rows & Avg Cols', 'rrrrr', 
    '''select 
    count(*),
    sum(head),
    sum(cols),
    round(avg(rows),1),
    round(avg(cols),1)
  from tab''')

table('tabshist', 'Rows & Tables & Header & Avg Columns', 'rrrr', '''
  select
    case 
      when rows <= 10 then 10
      when rows > 10 and rows <= 20 then 20
      when rows > 20 and rows <= 30 then 30
      when rows > 30 and rows <= 40 then 40
      when rows > 40 and rows <= 50 then 50
      when rows > 50 and rows <= 100000 then 100000
    end,
    count(*),
    round(1.0*sum(head)/count(*), 2),
    round(avg(cols), 1)
    from tab group by 1''')
 
# number of tables and cols after processing i.e. without ignored oolumns, such as all number, null
table('tabsafter', 'Tables & Columns & Avg Columns', 'rrrr', '''
  select
  count(distinct sel.tab) as tabs,
  count(sel.col) as cols, 
  round((count(sel.col) * 1.0) / count(distinct sel.tab), 1) as cols_per_tab
  from sel join tab on sel.tab = tab.id group by src ''')


# errors
table('tabserrors', 'Error & Number', 'lr', """
  select msg, count(*) from err where typ = 'tab' group by msg""")

#- select('tables: rows and cols');
#- select rows > 50, count(*), round(avg(cols), 2), round(avg(head), 2)
#- from tab group by rows > 50;

#- select('tables with at least one col with at least 10 distinct values and some type fraction >= 0.8:');
#- select count(distinct col.tab) from col join sel on col.tab = sel.tab and col.col = sel.col 
#- where ndist >= 3 and frac >= 0.8;

# most frequent values and types
table('tabsvcnt', 'Value & $N_V$ & $N_T$ & Types', 'p{2cm}rrp{11cm}', """
  select val, nv, nt, typs from vcnt order by nv desc limit 10""")

# tables: number of candidate reference tables (some col: ndist >= 10 and sel == 1
table('tabscandidates', 'Candidate Tables', 'r', '''
  select count(distinct tab) from sel where ndist >= 10 and sel = 1''')

# tables: number of reference tables (fraction subset test)
table('tabsreftabssubset', 'Number of Reference Tables', 'r', '''
  select count(distinct(k)) from sub join tab on sub.k = tab.id where comtyp >= 0''')

# tables: number of reference tables (strict subset test)
table('tabsrefstrict', 'Number of Reference Tables (strict)', 'r', '''
  select count(distinct(k)) from sub join tab on sub.k = tab.id where fracsubset = 1 and comtyp >= 0''')

# tables: ref tables by src
# table('tabsreftabsbysrc', 'Corpus & Reference Tables', 'lr', '''
#   select src, count(distinct(k)) from sub join tab on sub.k = tab.id group by src''')

# tables: how many by comtyp
table('tabsreftabsbycomtyp', 'Common Type & Tables', 'rr', '''
  select comtyp > 0, count(distinct(k)) from sub join tab on sub.k = tab.id group by comtyp > 0''')

# columns: distinct values and selectivity
table('tabscolsvalsandsel', 'Columns & Avg Number of Values & Avg Distinct Values & Selectivity', 'rrrr', '''
  select count(*), round(avg(nval), 1), round(avg(ndist), 1), round(avg(ndist/nval), 2) from sel
  where nval >= 3''')

# columns: avg coverage for ndist >= 3 and frac >= 0.8
table('tabscolscoverage', 'Columns & Avg Coverage', 'rr', '''
  select count(*), round(avg(cov), 3) 
  from col join sel on col.tab = sel.tab and col.col = sel.col 
  where ndist >= 3 and frac >= 0.8''')

# columns: avg coverage for ndist >= 3 and frac >= 0.8 and ndist = nval
table('tabscolscoverageforselone', 'Columns & Avg Coverage', 'rr', '''
  select count(*), round(avg(cov), 3) 
  from col join sel on col.tab = sel.tab and col.col = sel.col 
  where ndist >= 3 and frac >= 0.8 and ndist = nval''')

# columns: avg coverage for ndist >= 3 and frac = 1 and ndist = nval
table('tabscolscoverageforfracone', 'Columns & Avg Coverage', 'rr', '''
  select count(*), round(avg(cov), 3) 
  from col join sel on col.tab = sel.tab and col.col = sel.col 
  where ndist >= 3 and frac = 1 and ndist = nval''')

# columns with complete coverage
table('tabscolscompletecoverage', 'Columns', 'r', '''
  select count(distinct col) from col where cov = 1''')

# col types with fraction >= 0.8
table('tabscoltypes', 'Type & Number', 'lr', '''
  select typ, count(*) 
  from col 
  where frac >= 0.8
  group by typ 
  order by count(*) desc limit 20''')

#- select('sample of subset references: table k refenced by how many others:');
#- select distinct k, count(distinct l) from sub group by k order by count(*) desc limit 20;

#-select('sample of references with types (tab l col j references tab k col i):');
#-.width 5 2 5 2 50 50
#-select l, j, k, i, s, t from sub order by k, i, l, j limit 20 ;

# ref tables: how many refs by index
table('tabsreftabsbyindex', 'Index & Tables', 'rr', '''
  select sub.i, count(distinct(l)) as refs from sub group by sub.i order by refs desc limit 6''')

#- select('ref tables: how many refs by index');
#- select sub.i, count(distinct(l)) as refs
#- from sub group by sub.i order by refs desc;

## ref tables by src from to
#table('tabsreftabsbysrcfromto', 'llr', 'From & To & Tables', '''
#  select t1.src, t2.src, count(*)
#    from sub join tab t1 on sub.l = t1.id
#    join tab t2 on sub.k = t2.id
#    group by t1.src, t2.src''')

f.close()
