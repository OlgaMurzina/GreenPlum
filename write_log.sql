-- фукнция записи в журнал
create or replace function std6_97.f_write_log(p_log_type text, 
											   p_log_message text, 
											   p_location text)
	returns void
	language plpgsql
	volatile
as $$
declare
	v_log_type text;
	v_log_message text;
	v_sql text;
	v_location text;
	v_res text;	
begin
	v_log_type = upper(p_log_type);
	v_location = lower(p_location);

	if v_log_type not in ('error', 'info') 
		then
			raise exception 'illegal log type! use one of: error, info';
	end if;

	raise notice '%: %: <%> location[%]', clock_timestamp(), v_log_type, p_log_message, v_location;
	
	v_log_message := replace(p_log_message, '''', '''''');

	v_sql := 'insert into std6_97.logs(log_id, log_type, log_msg, log_location, is_error, log_timestamp, log_user)
				values (' || nextval('std6_97.log_id_seq') || ', 
					 ''' || v_log_type || ''', 
					   ' || coalesce('''' || v_log_message || '''', '''empty''') || ', 
					   ' || coalesce('''' || v_location || '''', 'null') || ', 
					   ' || case when v_log_type = 'error' then true else false end || ',
						current_timestamp, current_user);';
	
	raise notice 'insert sql is: %'	, v_sql; 
	v_res := dblink('adb_server', v_sql); 
end;
$$
execute on any;
