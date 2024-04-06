-- загрузка справочников в ext-таблицы (gpfdist), а потом сразу в целевые таблицы
-- справочники - region, product, chanel, price

create or replace function std6_97.f_load_from_spr(p_spr_table_name text)
	returns int8
	language plpgsql
	volatile 
as $$
declare 
	v_spr_table_name text;
	v_cnt int8=0;
	v_cnt_iter int8;
	v_file_name std6_97.spr%rowtype;
begin
	v_spr_table_name = 'std6_97.' || p_spr_table_name;
	for v_file_name in select file_name from v_spr_table_name
    loop
        -- здесь возможна обработка данных
	    perform std6_97.f_gpf_load(v_file_name) into v_cnt_iter;
	    raise notice 'Loaded % rows from %', v_cnt_iter, v_file_name;
	    v_cnt = v_cnt + v_cnt_iter;
        return setof record; -- возвращается текущая строка запроса
    end loop;
	raise notice 'All loaded % rows', v_cnt;
    return v_cnt;
end;
$$
execute on any;
