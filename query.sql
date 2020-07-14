.headers on
--.mode tabs

-- number of tables read from files, with rows and cols, before processing
-- numtables1.tex 
select "numtables1";
select src as Source, count(*) as Tables, avg(rows) as "Avg Rows", avg(cols) as "Avg Columns" 
  from tab group by Source;

-- number of tables and cols after processing i.e. without ignored oolumns, such as all number, null
-- numtables2.tex
select src, count(sel.col) as cols, count(distinct sel.tab) as tabs, 
  round((count(sel.col) * 1.0) / count(distinct sel.tab), 1) as cols_per_tab
  from sel join tab on sel.tab = tab.id group by src ;

-- 'tables: number of tables'
-- select src, cnt from num where obj = 'tab';

-- 'number of columns'
-- select src, cnt from num where obj = 'col';

-- .quit

select typ, msg, src, count(*) from err group by typ, msg, src;

-- select('tables: rows and cols');
-- select rows > 50, count(*), round(avg(cols), 2), round(avg(head), 2)
-- from tab group by rows > 50;

-- select('tables with at least one col with at least 10 distinct values and some type fraction >= 0.8:');
-- select count(distinct col.tab) from col join sel on col.tab = sel.tab and col.col = sel.col 
-- where ndist >= 3 and frac >= 0.8;

select('tables: number of candidate reference tables (some col: ndist >= 10 and sel == 1)');
select count(distinct tab) from sel where ndist >= 10 and sel = 1;

select('tables: number of reference tables (subset test)');
select count(distinct(k)) from sub;

select('tables: ref tables by src');
select src, count(distinct(k)) from sub join tab on sub.k = tab.id group by src;

select('tables: how many by comtyp');
select src, comtyp > 0, count(distinct(k)) from sub join tab on sub.k = tab.id group by src, comtyp > 0; 


select('columns: distinct values and selectivity:');
select nval >= 3, count(*), avg(nval), avg(ndist), avg(ndist/nval) from sel
group by nval >= 3;

select('columns: avg coverage for ndist >= 3 and frac >= 0.8:');
select count(*), avg(cov) 
from col join sel on col.tab = sel.tab and col.col = sel.col 
where ndist >= 3 and frac >= 0.8;

select('columns: avg coverage for ndist >= 3 and frac >= 0.8 and ndist = nval:');
select count(*), avg(cov) 
from col join sel on col.tab = sel.tab and col.col = sel.col 
where ndist >= 3 and frac >= 0.8 and ndist = nval;

select('col types with fraction >= 0.8:');
select typ as 'typ                                                            ', count(*) 
from col 
where frac >= 0.8
group by typ 
order by count(*) desc limit 20;

-- select('sample of subset references: table k refenced by how many others:');
-- select distinct k, count(distinct l) from sub group by k order by count(*) desc limit 20;

--select('sample of references with types (tab l col j references tab k col i):');
--.width 5 2 5 2 50 50
--select l, j, k, i, s, t from sub order by k, i, l, j limit 20 ;

--select('ref tables: how many refs by index');
--select sub.i, count(distinct(l)) as refs from sub group by sub.i order by refs desc limit 10;


-- select('ref tables: how many refs by index');
-- select sub.i, count(distinct(l)) as refs
-- from sub group by sub.i order by refs desc;

select('ref tables by src from to');
select t1.src, t2.src, count(*)
from sub join tab t1 on sub.l = t1.id
         join tab t2 on sub.k = t2.id
	group by t1.src, t2.src;

select('reftabs subset');
select count(distinct(k)) from sub;

select('reftabs LSH');
-- select k, count(distinct l) from lsh group by k;
select count(distinct k) from lsh;

select('reftabs subset Intersect lsh');
select count(*) from (
select distinct k from sub
intersect
select distinct k from lsh);

