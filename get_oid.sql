-- функция возвращения oid таблицы
create or replace function std6_97.f_get_oid(p_table text)
	returns oid
	language plpgsql
	volatile
as $$
declare
	v_table text;
	v_table_oid oid;
begin
	--вызов ф-ции унификации
	v_table := std6_97.f_unify_name(p_table);
	--получение oid таблицы p_table
	select c.oid 
	into v_table_oid 
	from pg_class c 
		inner join pg_namespace n 
		on c.relnamespace = n.oid
	where n.nspname || '.' || c.relname = v_table
	limit 1;

	return v_table_oid;
end;
$$
execute on any;
