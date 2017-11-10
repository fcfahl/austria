
DO
$$
DECLARE
i integer; 
n_targets int[];
BEGIN

	n_targets := (SELECT array_agg(id_target) FROM topo_targets WHERE id_target < 5);

	-- LOOP 1 (TARGET)
	FOR i IN 1 .. array_upper(n_targets, 1)
	LOOP		
		RAISE NOTICE 'The id is : % ', n_targets[i];
		SELECT * FROM jrc_get_routes(n_targets[i], 10000);
	END LOOP;
END
$$;

-- select * from topo_route_5km;