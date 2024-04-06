-- функция замены партиций со стартовой
create or replace function std6_97.f_switch_partition_start(p_table_name text, 
															p_partition_value timestamp, 
															p_switch_table_name text)
	returns void
	language plpgsql
	volatile
as $$
declare
  	v_partition_name text;
	v_switch_table_name text;
 	v_table_name text;
 	v_location text := 'std6_97.f_switch_partition_start';
	v_error text;
begin
	v_switch_table_name = f_unify_name(p_name := p_switch_table_name);
	v_table_name = f_unify_name(p_name := p_table_name);
	v_partition_name := f_partition_name_by_value(p_table_name, p_partition_value);
 
	perform f_switch_partition(
         p_table_name := v_table_name, 
         p_partition_name := v_partition_name, 
         p_switch_table_name := v_switch_table_name);  
	perform std6_97.f_write_log(
		p_log_type := 'info',
		p_log_message := 'end switch partitions for date '|| p_partition_value ||' in table '||v_table_name, 
		p_location := v_location);
end;
$$
execute on any;
