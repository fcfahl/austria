--         DROP TABLE IF EXISTS optimal_plant_residual_aggr_750kw;
-- CREATE TABLE optimal_plant_residual_aggr_750kw AS
        WITH
        total AS (
            SELECT id_target,
                SUM (live_methane)  AS live_total,
                SUM (crop_methane) AS crop_total
            FROM optimal_plant_resources_residual_750kw
            GROUP BY id_target
            ORDER BY id_target
        ),
        ratio AS (
            SELECT *,
		live_total * 1560000 * 0.3 AS live_demand,
		crop_total * 1560000 * 0.7 AS crop_demand
	    FROM total
        ),
        demand AS (
            SELECT *,
                CASE
                    -- manure not reach the minimin ratio amount
                    WHEN live_demand > 1560000 * 0.3 THEN  1560000 * 0.3
                    ELSE live_demand
                END AS live_methane_required,
                CASE
                    -- manure not reach the minimin ratio amount
                    WHEN crop_demand > 1560000 * 0.7 THEN  1560000 * 0.7
                    ELSE crop_demand
                END AS crop_methane_required                
	    FROM ratio
	  ),
        required AS (
            SELECT *,
                CASE
                    -- manure not reach the minimin ratio amount
                    WHEN live_methane_required = 1560000 * 0.3 THEN 0
                    WHEN crop_total + live_methane_required > 1560000  THEN  1560000 - live_total
                    ELSE 0
                END AS crop
	    FROM demand
	  ),	    

        aggregation AS (
            SELECT
                id_target,
                SUM (methane_total) AS methane_total_available,
                SUM (cost_harvest) AS cost_harvest_aggr,
                SUM (cost_ensiling) AS cost_ensiling_aggr,
                SUM (cost_manure) AS cost_manure_aggr,
                SUM (cost_total) AS cost_total_aggr
            FROM optimal_plant_resources_residual_750kw
            GROUP BY id_target
            ORDER BY id_target
        )
        SELECT * from crop
            ;