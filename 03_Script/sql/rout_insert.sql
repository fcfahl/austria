-- DROP TABLE IF EXISTS topo_route_tmp;
-- CREATE TABLE topo_route_tmp (id_building integer, id_target integer, cost double precision, distance double precision, length double precision);
-- 
-- SELECT AddGeometryColumn ('public', 'topo_route_tmp','geom', 3035, 'multilinestring', 2);

-- 1712, 1523

With
	farm as (
		SELECT 	a.id as id_node, b.id_building, ST_Distance(a.the_geom, b.geom) as dist 
		FROM 	topo_roads_noded_vertices_pgr AS a, 
			(Select * from topo_targets where id_building = 1712) AS b
		WHERE 	ST_DWithin (a.the_geom, b.geom, 500)
		order by dist ASC limit 1
	),
	target as (
		SELECT 	a.id as id_node, b.id_target, ST_Distance(a.the_geom, b.geom) as dist 
		FROM 	topo_roads_noded_vertices_pgr AS a, 
			(Select * from topo_targets where id_target = 10) AS b
		WHERE 	ST_DWithin (a.the_geom, b.geom, 500)
		order by dist ASC limit 1
	),
	dijkstra as (
		SELECT
			dijkstra.*, topo_roads_noded.geom
		FROM	pgr_dijkstra
			(
			    'SELECT id, source::integer, target::integer, distance::double precision AS cost FROM topo_roads_noded', 
			    (SELECT id_node FROM target), (SELECT id_node FROM farm), false
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
	(SELECT id_building FROM farm),
	(SELECT id_target FROM target),
	(SELECT sum(dist) FROM farm),
	(SELECT dist FROM target),
	(SELECT ST_Length (geom) FROM line),
	(SELECT geom FROM line)
)
;

SELECT * FROM topo_route_tmp;



