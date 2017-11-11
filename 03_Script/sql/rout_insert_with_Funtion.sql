-- DROP TABLE IF EXISTS topo_route_tmp;
-- CREATE TABLE topo_route_tmp (id_building integer, id_target integer, cost double precision, distance double precision, length double precision);
-- 
-- SELECT AddGeometryColumn ('public', 'topo_route_tmp','geom', 3035, 'multilinestring', 2);

-- 1712, 1523, 1919

With
	nodes as (
		select * from jrc_get_ids_from_nodes (10, 1712)
	),
	dijkstra as (
		SELECT
			dijkstra.*, topo_roads_noded.geom
		FROM	pgr_dijkstra
			(
			    'SELECT id, source::integer, target::integer, distance::double precision AS cost FROM topo_roads_noded', 
			    (SELECT node_target FROM nodes), (SELECT node_farm FROM nodes), false
			    --(SELECT node_target FROM nodes), (SELECT node_farm FROM nodes), false
			) AS dijkstra
		LEFT JOIN
			topo_roads_noded
		ON
			(edge = id)
		ORDER BY
			seq
	),
	line as (
		SELECT ST_Multi(ST_Collect(geom)) as geom FROM dijkstra
	)
	
INSERT INTO topo_route_tmp (id_building, id_target, cost, distance, length, geom)
VALUES (
	(SELECT id_building FROM nodes),
	(SELECT id_target FROM nodes),
	(SELECT sum(dist) FROM nodes),
	(SELECT dist FROM nodes),
	(SELECT ST_Length (geom) FROM line),
	(SELECT geom FROM line)
);

SELECT * FROM topo_route_tmp;



