-- функция возвращает атрибуты заданной таблицы
create or replace function std6_97.f_get_table_attributes(p_table_name text)
	returns text
	language plpgsql
	volatile
as $$
declare 
	v_params text;
begin
	select coalesce('with (' || array_to_string(reloptions, ', ') || ')','')
	from pg_class 
	into v_params
	where oid = p_table_name::regclass;
	return v_params;
end;
$$
execute on any;

