-- загрузка таблицы-справочника по типу FULL
create or replace function std6_97.f_full_load(
						p_table_from text,
						p_table_to text,
						p_where text,
						p_truncate_tgt bool)
	returns int8
	language plpgsql
	security definer 
	volatile 
as $$
	declare 
		v_table_from text;
		v_table_to text;
		v_where text;
		v_cnt int8;
	begin
		v_table_from = std6_97.f_unify_name(p_name:=p_table_from);
		v_table_to = std6_97.f_unify_name(p_name:=p_table_to);
		v_where = coalesce(p_where, '1=1');
		if coalesce(p_truncate_tgt, false) is true
			then perform(std6_97.f_truncate_table(v_table_to));
		end if;
		execute 'insert into ' || v_table_to || ' select * from ' || v_table_from || ' where ' || v_where;
		get diagnostics v_cnt=row_count;
		raise notice '% rows inserted from % into %', v_cnt, v_table_from, v_table_to;
		return v_cnt;
	end;
$$
execute on any;
	
	

