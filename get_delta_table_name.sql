-- функция создания имени временной таблицы из имени целевой таблицы
create or replace function std6_97.f_get_delta_table_name(p_table_name text)
	returns text 
	language plpgsql
	volatile 
as $$
	declare
		v_full_table_name text;
		v_tmp_table_name text;
		v_table_name text;
		v_schema_name text;
	begin
		v_full_table_name = std6_97.f_unify_name(p_name:=p_table_name);
		v_schema_name = left(v_full_table_name, position('.' in v_full_table_name) - 1);
		v_schema_name = 'stg_' || v_schema_name;
		v_table_name = right(v_full_table_name, length(v_full_table_name) - position('.' in v_full_table_name));
		v_tmp_table_name = v_schema_name ||'.delta_' || v_table_name;
		return v_tmp_table_name;
	end;
$$
execute on any;
	

