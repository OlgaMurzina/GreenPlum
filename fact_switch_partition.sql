-- функция вставки в партицию данных из таблицы заменой данных в партиции
create or replace function std6_97.f_switch_partition(p_table_name text, 
													  p_partition_name text, 
													  p_switch_table_name text)
	returns void
	language plpgsql
	volatile
as $$
declare
  	v_partition_name text;
	v_switch_table_name text;
	v_table_name text;
	v_rank int;
	v_count int;
	v_sql text;
 	v_location text := 'std6_97.f_switch_partition';
	/*v_error text;*/
begin
	v_partition_name = f_unify_name(p_partition_name);
	v_switch_table_name = f_unify_name(p_switch_table_name);
	v_table_name = f_unify_name(p_table_name);
 	perform std6_97.f_write_log(
		p_log_type := 'info',
		p_log_message := 'start switch partition ' || v_partition_name || ' with table ' || v_switch_table_name, 
		p_location := v_location);
	
	select partitionrank
	from pg_partitions
	into v_rank
	where schemaname || '.' || tablename = lower(v_table_name) and partitiontablename = lower(v_partition_name);
  
	v_sql = 'select count(*)
			from ' || p_switch_table_name;
	
	execute v_sql into v_count;
	
	
	if v_count > 0 then 
	execute 'alter table '||v_table_name||' exchange partition for (rank('||v_rank||')) with table '||v_switch_table_name||' with validation;';
	end if;
	
	perform std6_97.f_write_log(
		p_log_type := 'info',
		p_log_message := 'end switch partition '||v_partition_name||' with table '||v_switch_table_name, 
		p_location := v_location);
 
end;
$$
execute on any;
