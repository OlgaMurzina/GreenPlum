/* pxf */
-- ex_plan from pg.plan
create external table std6_97.ex_plan
("date" date,
region varchar,
matdirec varchar,
quantity int4,
distr_chan varchar)
location ('pxf://gp.plan?profile=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern') on all 
format 'custom' (FORMATTER='pxfwritable_import')
encoding 'UTF-8';

-- ex.sales from pg.sales
create external table std6_97.ex_sales
("date" date,
region varchar,
material varchar,
distr_chan varchar,
quantity int4,
check_nm varchar,
check_pos varchar)
location ('pxf://gp.plan?profile=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern') on all 
format 'custom' (FORMATTER='pxfwritable_import')
encoding 'UTF-8';

/* gpfdist */
-- ex_price from file price.csv
create external table std6_97.ex_price
(material varchar,
region varchar,
distr_chan varchar,
price int4)
location ('gpfdist://172.17.178.113:8080/price.csv')
format 'csv' (delimiter ';' null'');

-- ex_chanel from chanel.csv
create external table std6_97.ex_chanel
(material varchar,
region varchar,
distr_chan varchar,
price int4)
location ('gpfdist://172.17.178.113:8080/chanel.csv')
format 'csv' (delimiter ';' null'');

-- ex_product from product.csv
create external table std6_97.ex_product
(like std6_97.product)
location ('gpfdist://172.17.178.113:8080/product.csv')
format 'csv' (delimiter ';' null'');

-- ex_region from region.csv
create external table std6_97.ex_region
(like std6_97.region)
location ('gpfdist://172.17.178.113:8080/region.csv')
format 'csv' (delimiter ';' null'');

-- ex_region column from std6.region from region.csv
create external table std6_97.ex_region_1
(like std6_97.region)
location ('gpfdist://172.17.178.113:8080/region.csv')
format 'csv' (delimiter ';' null'');

-- ex_product_1 writable from product.csv
create writable external table std6_97.ex_product_1
(like std6_97.product)
location ('gpfdist://172.17.178.113:8080/product.csv')
format 'csv' (delimiter ';' null'')
distributed by (brand);
-- нет прав на создание таблицы на запись



