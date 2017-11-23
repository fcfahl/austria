select * from plants_costs_total limit 100;
select * from plants_costs limit 100;


select * from optimal_plant_residual order by id_target, id_building  limit 300;
select * from optimal_plant_residual where id_target = 102 order by id_building limit 300;
select * from optimal_plant_residual where rank = 3 order by methane_100kw ASC limit 100;

select * from optimal_plant_residual_aggr  where rank = 3  limit 100;
select * from optimal_plant_location limit 100;
select * from optimal_plant_links limit 100;


select * from test_optimization_route  limit 300;


select * from route_distance_50km_250__ where id_target = 102  limit 300;

select * from optimal_plant_costs where cost_total_100kw > 0 order by cost_total_100kw limit 1000;

    
select * from optimal_plant_residual where id_target = 401 and plant_capacity = 100 order by id_aggregate, plant_capacity;

select * from plants_costs  limit 100;
select * from plants_costs_total where rank = 3  limit 100;

select * from site_clean  limit 100;
       
select * from optimal_plant_costs  where rank = 3  order by id_target limit 100;
select * from optimal_plant_costs_aggr limit 100;

select * from optimal_plant_location  limit 100;