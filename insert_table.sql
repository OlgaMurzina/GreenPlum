-- функция для вставки данных в таблицу из другой таблице с фильтрацией

create or replace function std6_97.f_insert_table(p_table_from_name text, 
												  p_table_to_name text, 
												  p_where text)
	returns int8
	language plpgsql
	volatile
as $$
declare
	v_table_from_name text;
	v_table_to_name text;
	v_sql text;
	v_cnt int8;

	v_location text := 'std6_97.f_insert_table';
	/*v_error text;*/
begin
	v_table_from_name = f_unify_name(p_table_from_name);
	v_table_to_name = f_unify_name(p_table_to_name);
	perform std6_97.f_write_log(
		p_log_type := 'info',
		p_log_message := 'start insert data from table '||v_table_from_name||' to '||v_table_to_name || ' with condition: '||p_where,
		p_location := v_location);
	if p_table_from_name = 'std6_97.traffic_ext' 
	then
		v_sql = 'insert into '|| v_table_to_name ||' select plant as store, 
				to_date(date, ' || quote_literal('dd.mm.yyyy') || ') as date,
				to_timestamp(time, ' || quote_literal('hh24miss') || ')::time as time,
				frame_id,
				quantity
				from '|| v_table_from_name || ' where '||p_where;
									
	else
		v_sql = 'insert into '|| v_table_to_name ||' select * from '|| v_table_from_name || 
		' where '||p_where;
	end if;
	execute v_sql;
	get diagnostics v_cnt = row_count;
	raise notice '% rows inserted from % into %', v_cnt, v_table_from_name, v_table_to_name;
	perform std6_97.f_write_log(
		p_log_type := 'info',
		p_log_message := 'end insert data from table '||v_table_from_name||' to '||v_table_to_name || ' with condition: '||p_where,
		p_location := v_location);
	return v_cnt;	
end;
$$
execute on any;
