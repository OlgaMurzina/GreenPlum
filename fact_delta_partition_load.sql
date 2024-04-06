-- функция загрузки таблицы фактов по схеме delta partition

create or replace function std6_97.f_load_delta_partitions(p_table_from_name text, 
														   p_table_to_name text, 
														   p_partition_key text, 
														   p_schema_name text, 
														   p_start_date timestamp, 
														   p_end_date timestamp)
	returns int8
	language plpgsql
	volatile
as $$
declare 
	v_table_from_name text;
	v_table_to_name text;
	v_load_interval interval;
	v_iterdate timestamp;
	v_where text;
	v_prt_table text;
	v_cnt_prt int8;
	v_cnt int8;
	v_start_date timestamp;
	v_end_date timestamp;
	v_partition_key text := 'to_date('||p_partition_key||', ' ||quote_literal('dd.mm.yyyy')|| ')';
	v_location text := 'std6_97.f_create_date_partition';
	v_error text;
begin	
	v_table_from_name = f_unify_name(p_name := p_table_from_name);
	v_table_to_name = f_unify_name(p_name := p_table_to_name);
	v_cnt = 0;
	perform f_create_date_partition(p_table_name := v_table_to_name, 
								    p_partition_value := p_end_date);
	v_load_interval = '1 month'::interval;
	v_start_date := date_trunc('month', p_start_date);
	v_end_date := date_trunc('month', p_end_date) + v_load_interval;
	if v_table_from_name = 'std6_97.traffic_ext' then
	--v_where = 'to_date('||p_partition_key||', ' ||quote_literal('dd.mm.yyyy')|| ') >='''||v_start_date||'''::date and to_date('||p_partition_key||', ' ||quote_literal('dd.mm.yyyy')|| ')<'''||v_iterdate||'''::timestamp';
		loop
			v_iterdate = v_start_date + v_load_interval;
			exit when (v_iterdate > v_end_date);
			v_prt_table = f_create_tmp_table(p_table_name := v_table_to_name);
			/*v_where = 'to_date('||p_partition_key||', ''dd.mm.yyyy'') >= '''||v_start_date||'''::date
					and to_date('||p_partition_key||', ''dd.mm.yyyy'') < '''||v_end_date||'''::date';*/
			v_where = v_partition_key ||'>='''||v_start_date||'''::timestamp and '||v_partition_key||'<'''||v_iterdate||'''::timestamp';
			v_cnt_prt = f_insert_table(p_table_from_name := v_table_from_name, p_table_to_name := v_prt_table, p_where := v_where );
			v_cnt = v_cnt + v_cnt_prt;
			perform f_switch_partition_start(p_table_name := v_table_to_name, p_partition_value := v_start_date, p_switch_table_name := v_prt_table); 
			execute 'drop table '||v_prt_table;
			v_start_date := v_iterdate;
		end loop;
	else
		loop
			v_iterdate = v_start_date + v_load_interval;
			exit when (v_iterdate > v_end_date);
			v_prt_table = f_create_tmp_table(p_table_name := v_table_to_name);
			v_where = p_partition_key ||'>='''||v_start_date||'''::timestamp and '||p_partition_key||'<'''||v_iterdate||'''::timestamp';
			v_cnt_prt = f_insert_table(p_table_from_name := v_table_from_name, p_table_to_name := v_prt_table, p_where := v_where);
			v_cnt = v_cnt + v_cnt_prt;
			perform f_switch_partition_start(p_table_name := v_table_to_name, p_partition_value := v_start_date, p_switch_table_name := v_prt_table); 
			execute 'drop table '||v_prt_table;
			v_start_date := v_iterdate;
		end loop;
	end if;
	return v_cnt;
end;
$$
execute on any;
