-- функция анализа таблицы
create or replace function std6_97.f_analyze_table(p_table_name text)
	returns void
	language plpgsql
	volatile
as $$
declare 
	v_table_name text;
	v_sql text;
begin 
	--вызов ф-ции унификации
	v_table_name = std6_97.f_unify_name(p_table_name);
	--вызов сбора статистики
	v_sql = 'analyze ' || v_table_name;
	execute v_sql;
end
$$
execute on any;
