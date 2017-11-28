select * from optimal_plant_resources_residual_500kw  where id_target > 101 order by id_target,id_building limit 1000;
select * from optimal_plant_resources_allocated_500kw   limit 100;

select * from optimal_plant_location_500kw  order by id_order limit 100;
select * from optimal_plant_links_500kw  where id_target > 101 order by id_target,id_building limit 1000;

select * from optimal_plant_resources_residual_500kw   order by id_building limit 1100;

select * from optimal_plant_residual_aggr_500kw  where id_target > 101 order by id_target limit 1000;


select id_target, sum(live_methane_used) from optimal_plant_links_500kw   group by id_target limit 100;
select id_target, sum(live_methane) from optimal_plant_residual_500kw   where id_target = 107 group by id_target limit 100;
select id_target, sum(live_methane) from optimal_plant_resources_allocated_500kw   where id_target = 107 group by id_target limit 100;
select sum(live_methane) from optimal_plant_resources_allocated_500kw   limit 100;


select * from optimal_plant_links_500kw  order by id_building limit 100




select COALESCE(a.live_methane_used,0) - COALESCE(b.live_methane,0)
from optimal_plant_links_500kw  a, optimal_plant_residual_500kw  b
where a.id_building = b.id_building  and a.id_target = b.id_target and b.id_target = 107



join optimal_plant_residual_500kw  b on a.id_building = b.id_building  and a.id_target = b.id_target
where a.id_building = b.id_building  and a.id_target = b.id_target and b.id_target = 107


		SELECT *
        FROM optimal_plant_residual_aggr_500kw 
        WHERE methane_demand >= 224000 AND cost_total_aggr > 0  AND rank = 3
        AND id_target NOT IN (
            SELECT DISTINCT id_target FROM optimal_plant_location_500kw   -- avoid getting duplicates
        )
        
        ORDER BY cost_total_aggr ASC
        LIMIT 1
            ;