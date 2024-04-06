-- функция наименования партиции по значению номера партиции
create or replace function std6_97.f_partition_name_by_value(p_table_name text, 
															 p_partition_value timestamp)
	returns text
	language plpgsql
	volatile
as $$
declare
	v_table_name text;
	v_partition_name text;
	v_location text := 'std6_97.f_partition_name_by_value';
	/*v_error text;*/
begin
	v_table_name = f_unify_name(p_name := p_table_name);

	select max(partname) 
	from f_partition_name_list_by_date(v_table_name,p_partition_value,p_partition_value)
	into v_partition_name;
 
	perform std6_97.f_write_log(
		p_log_type := 'info',
		p_log_message := 'partition name: ' || coalesce(v_partition_name,'{empty}'),
		p_location := v_location);
    return v_partition_name;
end;
$$
execute on any;
