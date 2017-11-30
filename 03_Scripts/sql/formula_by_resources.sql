
        WITH
        parameters AS (
            SELECT
            1714 AS manure_required,
            4000 AS crop_required,
            526000 AS methane_required
        ),
        available AS (
            SELECT id_target, 
                SUM (manure) *.005 AS manure_available,
                SUM (crop_production) AS crop_available
            FROM optimal_plant_resources_residual_500kw
            GROUP BY id_target
            ORDER BY id_target
        ),
        required AS (
            SELECT a.id_target, 
		a.manure_available,
                b.manure_required,
		CASE 
			WHEN a.manure_available > b.manure_required THEN b.manure_required
			ELSE a.manure_available
                END AS manure_used,
                a.manure_available - b.manure_required AS manure_residual,
    
                a.crop_available,
                b.crop_required,
                a.crop_available - b.crop_required AS crop_demand
            FROM available AS a, parameters AS b
        ),
        manure_methane AS (
            SELECT id_target, manure_available, manure_required, manure_used, manure_residual,
                manure_used * 14.4 AS manure_methane_produced,
                CASE 
			WHEN manure_residual < 0 THEN (manure_available * 14.4 - manure_required * 14.4) * -1
			ELSE 0
                END AS manure_methane_residual
            FROM required
        ),
        crop_methane_missing AS (
            SELECT a.id_target, b.crop_available, b.crop_required,
		a.manure_methane_residual * 14.4 AS methane_lacking_from_manure,
		a.manure_methane_residual / 125.4 AS crop_additional
            FROM manure_methane AS a, required AS b
            WHERE a.id_target = b.id_target
        ),
        crop_methane AS (
            SELECT a.*,
		b.crop_available, b.crop_additional,
		b.crop_required + b.crop_additional AS crop_required
            FROM manure_methane AS a, crop_methane_missing AS B
            WHERE a.id_target = b.id_target
        ),
        total_methane AS (
            SELECT *,
		manure_used * 14.4 AS methane_from_manure,
		crop_required * 125.4 AS methane_from_crop,
		manure_used * 14.4 + crop_required * 125.4 AS methane_total_produced
            FROM crop_methane
        )
               


--       manure_available * 14.4 AS manure_methane_produced,
--                 manure_required * 14.4 AS manure_methane_required,
--                 CASE 
--                 WHEN manure_demand < 0 THEN (manure_available * 14.4 - manure_required * 14.4) * -1
--                 ELSE 0
--                 END AS manure_methane_residual,
-- 		crop_available,
--                 crop_required,
--                 crop_demand,
--                 crop_available * 125.4 AS crop_methane_produced,
--                 crop_required * 125.4 AS crop_methane_required   

        
--         methane AS (
--             SELECT a.id_target, a.manure_available,
--                 a.manure_available * 14.4 AS manure_methane,
--                 b.manure_required * 14.4 AS manure_methane_required,
--                 a.crop_available,
--                 a.crop_available * 125.4 AS crop_methane,
--                 b.crop_required * 125.4 AS crop_methane_required,
--                 b.methane_required
--             FROM available AS a, requirements AS b
--         ),
--         
--         conditionals AS (
--             SELECT *
-- -- 		case 
-- -- 		when manure_available > manure_required then 1
-- -- 		else 0
-- -- 		end as manure_demand,
-- -- 
-- --                 a.manure_available * 14.4 AS manure_methane,
-- --                 a.crop_available * 125.4 AS crop_methane,
-- --                 b.methane_required
--             FROM methane
--         )
--         ,

--          methane_total AS (
--             SELECT *,
--             manure_methane + crop_methane AS methane_available
-- 		
--             FROM methane_produced
--         )
        select * from total_methane  limit 100
            ;