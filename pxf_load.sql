-- функция загрузки таблицы по pxf

create or replace function std6_97.f_pxf_load(tab text)
	returns int4
	language plpgsql
	volatile
as $$
declare 
	exe_sql text ;
	ans int4 ;
	f_tab text = 'std6_97.' || tab_name  ;
	e_tab text = 'std6_97.ex_' || tab_name;
	pxf text = 'pxf://gp.' || tab_name || '?profile=jdbc&jdbc_driver=org.postgresql.driver&db_url=jdbc:postgresql:' ||
			   '//192.168.214.212:5432/postgres&user=intern&pass=intern';
begin
	execute 'drop external table if exists ' || e_tab;
	execute 'truncate table ' || f_tab;
	exe_sql = 'create external table ' || e_tab || '
		(like ' || tab_name || ') location (''' ||
		pxf || ''') on all format ''custom'' (formatter=''pxfwritable_import'')
		encoding ''utf8'' ';	
	
	raise notice '%', exe_sql ;
	execute exe_sql ;
	exe_sql = 'insert into ' || f_tab || ' select * from ' || e_tab;
	raise notice '%',exe_sql ;
	execute exe_sql;
	exe_sql = 'select count(1) from ' || f_tab ;
	execute exe_sql into ans;
	
	return ans;
end;
$$
execute on any;
