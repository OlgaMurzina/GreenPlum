-- удаление записей из существующей таблицы
create or replace function std6_97.f_truncate_table(p_table_name text)
	returns void
	language plpgsql
	volatile
as $$
	declare 
		v_table_name text;
		v_sql text;
	begin
		v_table_name = std6_97.f_unify_name(p_name:=p_table_name);
		v_sql = 'truncate table ' || v_table_name ||';';
		execute v_sql;
	end;
$$
execute on any;

