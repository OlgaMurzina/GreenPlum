-- функция наименовани партиций по списку дат
create or replace function std6_97.f_partition_name_list_by_date(p_table_name text, 
																 p_partition_start timestamp, 
																 p_partition_end timestamp)
	returns table (partname text, partrangestart timestamp, partrangeend timestamp)
	language sql
	volatile
as $$
	select partitiontablename::text, 
		   split_part(partitionrangestart, '::', 1)::timestamp as partitionrangestart,
		   split_part(partitionrangeend, '::', 1)::timestamp as partitionrangeend
	from pg_partitions
    where lower(schemaname||'.'||tablename) = lower(p_table_name) and partitionisdefault = false
	 	  and ((split_part(partitionrangestart, '::', 1)::timestamp between to_timestamp(to_char(p_partition_start, 'yyyymmddhh24miss'), 'yyyymmddhh24miss') and to_timestamp(to_char(p_partition_end, 'yyyymmddhh24miss'), 'yyyymmddhh24miss')) 
		  or ( split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second' between to_timestamp(to_char(p_partition_start, 'yyyymmddhh24miss'), 'yyyymmddhh24miss') and to_timestamp(to_char(p_partition_end, 'yyyymmddhh24miss'), 'yyyymmddhh24miss')) 
		  or ( to_timestamp(to_char(p_partition_start, 'yyyymmddhh24miss'), 'yyyymmddhh24miss') between split_part(partitionrangestart, '::', 1)::timestamp and split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second') 
		  or ( to_timestamp(to_char(p_partition_end, 'yyyymmddhh24miss'), 'yyyymmddhh24miss') between split_part(partitionrangestart, '::', 1)::timestamp and split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second'))
   order by partitionposition;
$$
execute on any;
