create table std6_97.table2
(id int,
not_id int)
distributed by (id);

insert into std6_97.table2 
select row_number() over() as id, not_id
from (select pg_catalog.generate_series(1, (pg_catalog.random() * 50) :: integer) as not_id 
from generate_series(1, 10000) as id) a;

-- проверка распределения по сегментам
select gp_segment_id, count(1)
from std6_97.table2
group by 1;

-- коэффициент перекоса - отношение стандартного отклонения к общему кол-ву записей в %
select (gp_toolkit.gp_skew_coefficient('std6_97.table2'::regclass)).skccoeff;

select not_id, 
	   count(*)
from std6_97.table2
group by 1;

/* Дистрибуция по not_id */
-- меняем ключ дистрибуции на not_id
alter table std6_97.table2
	set with (reorganize=true)
	distributed by (not_id);

-- распределение
select gp_segment_id, count(*)
from std6_97.table2
group by 1;


-- коэффициент перекоса
select (gp_toolkit.gp_skew_coefficient('std6_97.table2'::regclass)).skccoeff;

-- создаем таблицу вида appendoptimized с настройками
create table std6_97.table3
(
id integer,
not_id integer
)
	with(
	appendoptimized=true, 
	orientation=column, 
	compresstype=zstd, 
	compresslevel=1)
distributed by (id);

-- создаем таблицу с ключом дистрибуции - репликация
create table std6_97.table4
(
fieldl int,
field2 text
)
distributed replicated;

insert into std6_97.table4
select a, md5(a::text)
from generate_series(1,2000) a;

-- column gp_segment_id doesn't exist, тк на каждом сегменте хранится
-- полная копия нашей таблицы, сегменты не выделяются
select gp_segment_id, count(1) 
from std6_97.table4 
group by 1;

-- создаем таблицу с ключом дистрибуции - рандомли
create table std6_97.table5
(
fieldl int,
field2 text
)
distributed randomly;

-- если вставлять записи большими порциями, то распределение равномерно
-- если вставлять записи по одной, то распределение пройдет в 1 сегмент
insert into std6_97.table5
	select a, md5(a::text)
	from generate_series(1,10000) a;
	
select gp_segment_id, count(1)
from std6_97.table5
group by 1;

-- удаляем данные, но не таблицу
truncate table std6_97.table5;

insert into std6_97.table5
	select 1, md5(1::text);

select gp_segment_id, count(1)
from std6_97.table5
group by 1;

/* CONSTREINTS */
/* NULL */
create table std6_97.table6
(
field1 int,
field2 text not NULL
)
distributed by(field1);

-- ERROR
insert into std6_97.table6
	select a, NULL
	from generate_series(1,2000) a;


create table std6_97.table7
(
fieldl int,
field2 text DEFAULT 'hello'
)
distributed by(fieldl);


insert into std6_97.table7
	select a
	from generate_series(1,2000) a;

select * from std6_97.table7;


/* PRIMARY KEY */
create table std6_97.table8
(
fieldl int primary key,
field2 text
)
distributed by(fieldl);

insert into std6_97.table8
(fieldl, field2)
values(1, 'PRIMARY KEY test');

/* CHECK */
create table std6_97.table9
(
field1 int check (field1 > 10),
field2 text
)
distributed by(field1);

-- ERROR
insert into std6_97.table9
(field1, field2)
values(1, 'check test');


/* PARTITION BY RANGE */
create table std6_97.sales
(
	id int,
	dt date,
	amt decimal(10,2)
)
distributed by (id)
partition by range (dt)
(
	start (date '2016-01-01') inclusive
	end (date '2017-01-01') exclusive
	every (interval '1 month')
);

insert into std6_97.sales
  values(1, '2016-01-02'::date, 134);


insert into std6_97.sales
  values(2, '2016-10-02'::date, 145);

select * from std6_97.sales;

select * from std6_97.sales_1_prt_1;

select * from std6_97.sales_1_prt_10;

-- ERROR
insert into std6_97.sales
  values(3, '2018-01-02'::date, 200);

/* PARTITION WITH DEFAULT PARTITION */	
drop table std6_97.sales;

create table std6_97.sales
(
	id int,
	dt date,
	amt decimal(10,2)
)
distributed by (id)
partition by range (dt)
(
	start (date '2016-01-01') inclusive
	end (date '2017-01-01') exclusive
	every (interval '1 month'),
	default partition def
);

insert into std6_97.sales
  values(1, '2018-01-02'::date, 200);

select * from std6_97.sales_1_prt_def;

/* SPLIT PARTITION */
-- вырезаем данные из дефолтной партиции, если их скопилось много
alter table std6_97.sales
  split default partition start ('2018-01-01') end ('2018-02-01') exclusive;

 -- получить список всех партиций таблицы
select
	partitiontablename,
	partitionrangestart,
	partitionrangeend
from pg_partitions
where tablename = 'sales'
	and schemaname = 'std6_97'
order by partitionrangestart;

select * from std6_97.sales_1_prt_def; /* change to your partition name */

/* иногда удобно заменить партицию данными из отдельной таблицы
 * для этого используют команду exchange partition */
-- создаем временную таблицу
create table std6_97.sales_2016
(
	id int,
	dt date,
	amt decimal(10,2)
)
distributed by (id);

-- чистим таблицу sale
truncate table std6_97.sales;

-- вставляем туда данные
insert into std6_97.sales_2016
 values(1, '2016-10-02'::date, 145);

-- заменяем партицию на содержимое временной таблицы
alter table std6_97.sales
exchange partition for (date '2016-10-01')
with table std6_97.sales_2016
with validation;

select * from std6_97.sales;

-- exchange работает и по данным типа int
create table std6_97.rank
(
	id int,
	rank int,
	year int,
	gender char(1),
	count int
)
distributed by (id)
partition by range (year)
(
start (2006) end (2016) every (1),
default partition extra
);

select
	partitiontablename,
	partitionrangestart,
	partitionrangeend
from pg_partitions
where tablename = 'rank'
	and schemaname = 'std6_97'
order by partitionrangestart;

/* PARTITION BY LIST */
create table std6_97."list"
(
	id int,
	rank int,
	year int,
	gender char(1),
	count int
)
distributed by (id)
partition by list (gender)
(
	partition girls values ('f'),
	partition boys values ('m'),
	default partition other
);


select
	partitiontablename,
	partitionname,
	partitionlistvalues
from pg_partitions
where tablename = 'list'
  and schemaname = 'std6_97'
order by partitionrangestart;

/* appendoptimize table */

/* ORIENTATION = ROW */
CREATE TABLE std6_97.lineitem_row (
	l_shipdate date NULL,
	l_orderkey int4 NULL,
	l_discount numeric(19, 4) NULL,
	l_extendedprice numeric(19, 4) NULL,
	l_suppkey int4 NULL,
	l_quantity int4 NULL,
	l_returnflag varchar(1) NULL,
	l_partkey int4 NULL,
	l_linestatus varchar(1) NULL,
	l_tax numeric(19, 4) NULL,
	l_commitdate date NULL,
	l_receiptdate date NULL,
	l_shipmode varchar(10) NULL,
	l_linenumber int4 NULL,
	l_shipinstruct varchar(25) NULL,
	l_comment varchar(44) NULL
)
WITH (
appendonly=true,
orientation=row
)
DISTRIBUTED BY (l_orderkey);


insert into std6_97.lineitem_row
	select * from std6_97.lineitem_ext; /* not table */

select pg_size_pretty(pg_total_relation_size('std6_97.lineitem_row')) as size;


/* ORIENTATION = ROW 
	 COMPRESSION = ON
*/
CREATE TABLE std6_97.lineitem_row_compr (
	l_shipdate date NULL,
	l_orderkey int4 NULL,
	l_discount numeric(19, 4) NULL,
	l_extendedprice numeric(19, 4) NULL,
	l_suppkey int4 NULL,
	l_quantity int4 NULL,
	l_returnflag varchar(1) NULL,
	l_partkey int4 NULL,
	l_linestatus varchar(1) NULL,
	l_tax numeric(19, 4) NULL,
	l_commitdate date NULL,
	l_receiptdate date NULL,
	l_shipmode varchar(10) NULL,
	l_linenumber int4 NULL,
	l_shipinstruct varchar(25) NULL,
	l_comment varchar(44) NULL
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (l_orderkey);


insert into std6_97.lineitem_row_compr
  select * from std6_97.lineitem_ext; /* not table */

select pg_size_pretty(pg_total_relation_size('std6_97.lineitem_row_compr')) as size;

CREATE TABLE std6_97.lineitem_column_compr (
	l_shipdate date NULL,
	l_orderkey int4 NULL,
	l_discount numeric(19, 4) NULL,
	l_extendedprice numeric(19, 4) NULL,
	l_suppkey int4 NULL,
	l_quantity int4 NULL,
	l_returnflag varchar(1) NULL,
	l_partkey int4 NULL,
	l_linestatus varchar(1) NULL,
	l_tax numeric(19, 4) NULL,
	l_commitdate date NULL,
	l_receiptdate date NULL,
	l_shipmode varchar(10) NULL,
	l_linenumber int4 NULL,
	l_shipinstruct varchar(25) NULL,
	l_comment varchar(44) NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (l_orderkey);

insert into std6_97.lineitem_column_compr
	select * from std6_97.lineitem_ext;
	
select pg_size_pretty(pg_total_relation_size('std6_97.lineitem_column_compr')) as size;


/* Homework2 */
-- table plan
CREATE TABLE std6_97.plan (
	"date" date not null,
	region varchar(20) not null,
	matdirec varchar(20) not null,
	quantity int4,
	distr_chan varchar(100) not null
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (distr_chan)
partition by range (date)
(
	start (date '2021-01-01') inclusive
	end (date '2021-08-01') exclusive
	every (interval '1 month'),
	default partition def
);

drop table std6_97.sales;

-- table sales
CREATE TABLE std6_97.sales (
	"date" date,
	region varchar(20),
	material varchar(20),
	distr_chan varchar(100),
	quantity int4,
	check_nm varchar(100) not null,
	check_pos varchar(100) not null
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (distr_chan)
partition by range (date)
(
	start (date '2021-01-01') inclusive
	end (date '2021-08-01') exclusive
	every (interval '1 month'),
	default partition def
);

-- table price
CREATE TABLE std6_97.price (
	material varchar(20),
	region varchar(20),
	distr_chan varchar(100),
	price int4
)
distributed replicated;

-- table chanel
CREATE TABLE std6_97.chanel (
	distr_chan varchar(100),
	txtsh varchar(20)
)
distributed replicated;

-- table region
CREATE TABLE std6_97.region (
	region varchar(20),
	txt text
)
distributed replicated;

-- table product
CREATE TABLE std6_97.product (
	material varchar(20),
	asgrp varchar(20),
	brand int4,
	matcateg varchar(20),
	matdirec int4,
	txt text
)
distributed replicated;

select extract (month from date::date) as month
from gp.sales
group by month





