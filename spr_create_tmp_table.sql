-- создает временную таблицу

create or replace function std6_97.f_create_tmp_table(p_table_name text)
	returns text
	language plpgsql
	volatile
as $$
	declare 
		
		v_tmp_t_name text;
		v_sql text;
		v_storage_param text;
		v_dist_key text;
	
		v_location text := 'std6_97.f_create_tmp_table';
		/*v_error text;*/	
	begin		
		v_tmp_t_name = p_table_name||'_tmp';
		v_storage_param = f_get_table_attributes(p_table_name);
		v_dist_key = f_get_distribution_key(p_table_name);
	
		perform std6_97.f_write_log(
			p_log_type := 'info',
			p_log_message := 'start creating temp table '||v_tmp_t_name,
			p_location := v_location);
	
		v_sql := 'drop table if exists '|| v_tmp_t_name ||';
					create table ' || v_tmp_t_name || ' (like '|| p_table_name ||') ' ||v_storage_param||' '||v_dist_key||';';
		execute v_sql;
	
		perform std6_97.f_write_log(
			p_log_type := 'info',
			p_log_message := 'end creating temp table '||v_tmp_t_name,
			p_location := v_location);
		
		return v_tmp_t_name;
	
	end;
$$
execute on any;

