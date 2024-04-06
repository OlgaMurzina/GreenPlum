-- функция загрузки справочников через ext_table в целевую таблицу по gpfdist

create or replace function std6_97.f_gpf_load(tab_name text)
	returns int4
	language plpgsql
	volatile
as $$	
declare 
	exe_sql text ;
	ans text ;
	full_tab_name text ;
	ext_tab_name text;
	gpfdist text = 'gpfdist://172.17.178.113:8080/' || tab_name || '.csv';
begin
	full_tab_name = 'std6_97.' || tab_name ;
	ext_tab_name = 'std6_97.ex_' || tab_name;
	raise notice 'Ext_table_name is %', ext_tab_name;
	execute 'truncate table ' || full_tab_name;
	execute 'drop external table if exists ' || ext_tab_name;

	exe_sql = 'create external table ' || ext_tab_name || ' (like ' || full_tab_name || ') 
				location (''' || gpfdist || ''') on all
				format ''csv'' ( header delimiter '';'' null '''' escape ''"'' quote ''"'' )
				encoding ''utf8''' ;
	raise notice '%', exe_sql ;
	execute exe_sql ;

	exe_sql = 'insert into ' || full_tab_name || ' select * from ' || ext_tab_name ;
	raise notice '%', exe_sql ;
	execute exe_sql;

	exe_sql = 'select count(1) from ' || full_tab_name ;
	raise notice '%', exe_sql;
	execute exe_sql into ans;

	return ans;
end
$$
execute on any;
