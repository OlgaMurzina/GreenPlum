-- унификация имен таблиц
create or replace function std6_97.f_unify_name(p_name text)
	returns text 
	language plpgsql
	volatile
as $$
	declare
	begin
		return lower(trim(translate(p_name, ';/''', '')));
	end;
$$
execute on any;
	