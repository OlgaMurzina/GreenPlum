create schema std6_97;

create table std6_97.table1
(field1 int,
field2 text)
distributed by (field1);

insert into std6_97.table1 
select a, md5(a::text)
from generate_series(1, 1000) a;

select gp_segment_id, count(1)
from std6_97.table1
group by 1;

