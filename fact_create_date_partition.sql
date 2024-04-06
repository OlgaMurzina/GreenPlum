-- функция создает партицию по интервалу партицирования

create or replace function std6_97.f_create_date_partition(p_table_name text, 
                                                           p_partition_value timestamp)
	returns void
	language plpgsql
	volatile
as $$
	declare 
		v_table_name text;
		v_cnt_partitions int4;
		v_partition_end_sql text;
		v_partition_end timestamp;
		v_interval interval;
		v_day_interval interval;
		v_ts_format text := 'yyyy-mm-dd hh24:mi:ss';
		v_new_partition_end timestamp;
	
		v_location text := 'std6_97.f_create_date_partition';
		v_error text;		
	begin
		
		v_table_name = f_unify_name(p_table_name);
	
		perform std6_97.f_write_log(
			p_log_type := 'info',
			p_log_message := 'start creating partitions for table '||v_table_name,
			p_location := v_location);
	
		select count(*) 
		from pg_partitions p 
		where p.schemaname||'.'||p.tablename = lower(v_table_name)
		into v_cnt_partitions;
	
		if v_cnt_partitions >= 1 then 
			loop 
				select partitionrangeend 
					from (
						select p.*, rank() over (order by partitionrank desc) rnk from pg_partitions p
						where p.partitionrank is not null and p.schemaname||'.'||p.tablename = lower(v_table_name)
						) q
					into v_partition_end_sql
					where rnk = 1;
				
			execute 'select '||v_partition_end_sql into v_partition_end;
			exit when v_partition_end >= p_partition_value;
			
			v_interval := '1 month'::interval;
			v_day_interval := '1 day'::interval;
			v_new_partition_end = to_char(date_trunc('month', v_partition_end+v_interval)/*-v_day_interval*/, v_ts_format);
			
			
			execute 'alter table '||v_table_name||' split default partition
					start ('''||v_partition_end/*+v_day_interval*/||''') end ('''||v_new_partition_end||'''::timestamp)';
		
			end loop;
		end if;
	
		perform f_write_log(
			p_log_type := 'info',
			p_log_message := 'end creating partitions for table '||v_table_name,
			p_location := v_location);
	
	end;
$$
execute on any;

