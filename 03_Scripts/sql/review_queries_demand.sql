-- select * from optimal_plant_residual_aggr_750kw limit 100;
-- select * from optimal_plant_location_750kw order by id_order limit 100;
-- select * from optimal_plant_links_750kw  limit 100;


  WITH
        last_record AS (
            SELECT *
            FROM optimal_plant_location_750kw
            ORDER BY id_order DESC
            LIMIT 1
        ),
         manure AS (
            SELECT *
            FROM (
                SELECT a.id_aggregate, a.manure, a.live_methane, b.live_required,
                SUM (a.live_methane) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS live_methane_aggregated,
                row_number () OVER (ORDER BY a.length ASC) AS live_row
                FROM optimal_plant_resources_residual_750kw AS a, last_record AS b
                WHERE a.id_target = b.id_target AND a.live_methane > 0
                ) AS f, live_columns AS g
            WHERE live_methane_aggregated <= g.live_aggr + 1 -- grab the next value of the sequence
        ),
        crop AS (
            SELECT *
            FROM (
                SELECT a.id_aggregate, a.id_building, a.id_target, a.length, a.crop_production, a.crop_methane, b.crop_required,
                SUM (a.crop_methane) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS crop_methane_aggregated,
                row_number () OVER (ORDER BY a.length ASC) AS crop_row
                FROM optimal_plant_resources_residual_750kw AS a, last_record AS b
                WHERE a.id_target = b.id_target AND a.crop_production > 0
                ) AS f, crop_columns AS g
            WHERE f.crop_row <= g.crop_row + 1 -- grab the next value of the sequence
        ),
        cost_total AS (
            SELECT a.id_aggregate, a.cost_harvest, a.cost_ensiling, a.cost_manure, a.cost_total
            FROM optimal_plant_resources_residual_750kw AS a, crop AS b
            WHERE a.id_aggregate = b.id_aggregate
        )
	select * from live_columns

        ;