-- обновление таблиц-справочников методом delta-merge
create or replace function std6_97.f_delta_merge_load(  p_table_from text,
														p_table_to text,
														p_where text,
														p_merge_key text)
	returns int8
	language plpgsql
	security definer 
	volatile 
as $$
	declare
		v_full_table_name text;
		v_tmp_table_name text;
		v_ext_table_name text;
		v_table_from text;
		v_table_to text;
		v_buffer_table text;
		v_table_cols text;
		v_table_key text;
		v_merge_sql text;
		v_cnt int8;
	begin
		v_full_table_name = std6_97.f_unify_name(p_name:=p_table_to);
		v_tmp_table_name = std6_97.f_get_delta_table_name(p_name:=p_table_to);
		v_ext_table_name = std6_97.f_get_ext_table_name(p_name:=p_table_to);
		perform std6_97.f_truncate_table(p_table_name:=v_tmp_table_name);
		perform std6_97.f_insert_table(p_table_from:=v_ext_table_name,
									   p_table_to:= v_tmp_table_name,
									   p_where:=p_where);
		v_table_from = std6_97.f_unify_name(p_name:=v_tmp_table_name);
		v_table_to = std6_97.f_unify_name(p_name:=v_full_table_name);
		-- создание временной таблицы
		v_buffer_table = std6_97.f_create_tmp_table(p_table_name:=v_table_to,
												    p_prefix_name:='buffer_');
		-- получение списка столбцов
		select string_agg(column_name, ',' order by ordinal_position) into v_table_cols
		from std6_97.columns
		where std6_97 || '.' || table_name = v_table_to;
		-- создание merge-скрипта для вставки в буферную таблицу результата
	    -- merge между v_table_to и v_table_from
		v_merge_sql = 'insert into ' || v_buffer_table || 'select ' || v_table_cols ||
					  ' from (select q.*, rank() over (partition by ' || p_merge_key || 
					  ' order by rnk) as rnk_f from (select ' || v_table_cols || ', "1" rnk from '||
					  v_table_from || ' f union all select ' || v_table_cols || ', "2" rnk from ' ||
					  v_table_to || ' t) q) qr where rnk_f=1;';
		execute v_merge_sql;
		perform std6_97.f_switch_def_partition(p_table_from_name:=v_buffer_table,
											   p_table_to_name:=v_table_to);
	end;
$$
execute on any;
	
			

