-- функция загрузки витрины

create or replace function std6_97.f_load_mart(p_date_from date, 
											   p_date_to date)
	returns int8
	language plpgsql
	volatile
as $$
declare
	v_sql text;
	v_return int8;
begin
	drop table if exists std6_97.mart;
	create table std6_97.mart
	with (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
	)
	as
		select s.store, 
			   s.txt, 
			   q1.turnover, 
			   q6.coupon_discount, 
			   q1.turnover - q6.coupon_discount as turnover_with_discount,
			   q1.items_quantity, 
			   q2.bills_quantity,
			   q3.traffic, 
			   q4.items_coupon_quantity, 
			   round(q4.items_coupon_quantity * 100 / q1.items_quantity, 1) as items_coupon_share,
			   round(q1.items_quantity / q2.bills_quantity, 2) as discount_items_rate,
			   round(q2.bills_quantity * 100 / q3.traffic, 2) as conversion_koef,
			   round(q1.turnover / q2.bills_quantity, 1) as avg_bill,
			   round(q1.turnover / q3.traffic, 1) as avg_income_per_visitor		
		from std6_97.stores as s
		left join ( 
			select bh.plant, 
					sum(bi.rpa_sat) turnover, 
					sum(bi.qty) items_quantity
			from std6_97.bills_head as bh
			inner join std6_97.bills_item as bi
			on bh.billnum = bi.billnum
			where bh.calday between p_date_from and p_date_to
			group by bh.plant
		) q1 
		on s.store = q1.plant
		left join ( 
			select plant, count(*) as bills_quantity
			from std6_97.bills_head
			where calday between p_date_from and p_date_to
			group by plant
		) q2 
		on s.store = q2.plant
		left join ( 
			select store, sum(quantity) as traffic
			from std6_97.traffic
			where date between p_date_from and p_date_to
			group by store
		) q3 
		on s.store = q3.store
		left join ( 
			select store, count(*) as items_coupon_quantity
			from std6_97.coupons
			where date between p_date_from and p_date_to
			group by store
		) q4 
		on s.store = q4.store
		left join ( 
			select c.store, 
				   sum(case when promo_type = '001' 
				   				then discount
							when promo_type = '002' 
								then (bi.rpa_sat / bi.qty) * discount / 100
					   end
					) coupon_discount
			from std6_97.coupons as c
			inner join (
				select billnum, material, 
					   rpa_sat, 
					   qty, 
					   row_number() over (partition  by billnum, material) as  rn
				from std6_97.bills_item 		
			) bi
			on c.bill_number = bi.billnum and c.material = bi.material
			inner join std6_97.promos  pr
			on c.id_promo = pr.id_promo
			
			where c.date between p_date_from and p_date_to and rn = 1
			group by c.store
		) q6 
		on s.store = q6.store
		order by s.store
		distributed by (store);

	select count(*) into v_return from std6_97.mart;
	return v_return;
end
$$
execute on any;
