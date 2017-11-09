DROP TABLE IF EXISTS tmp_topo_route_array;
CREATE TABLE tmp_topo_route_array AS

SELECT  dijkstra.*, ST_Union(topo_roads_noded.geom)
FROM pgr_dijkstra('SELECT id, source::integer, target::integer, distance::double precision AS cost FROM topo_roads_noded', 1, array[10, 50, 75, 100, 1000], false) AS dijkstra
LEFT JOIN
	topo_roads_noded
ON
	(edge = id)
GROUP BY end_vid, seq, path_seq, node, edge, cost, agg_cost, topo_roads_noded.geom;


select * from tmp_topo_route_array;

DROP TABLE IF EXISTS tmp_topo_route_array;
CREATE TABLE tmp_topo_route_array AS
With
	target as
	(
		SELECT 	a.id as id_node, b.id_target, ST_Distance(a.the_geom, b.geom) as dist 
		FROM 	topo_roads_noded_vertices_pgr AS a, 
			(Select id_target, geom from topo_targets where id_target = 1) AS b
		WHERE 	ST_DWithin (a.the_geom, b.geom, 500)
		order by dist ASC limit 1
	),
	farm as
	(
	    	SELECT DISTINCT array_agg(a.id_building) as id_node
			FROM (Select * from topo_targets where id_building > 0) as a
			JOIN (Select * from topo_targets where id_target = 1) as b
			ON ST_DWithin (a.geom, b.geom, 50000)
    		
	),
	dijkstra as (
		SELECT
			dijkstra.*, topo_roads_noded.geom
		FROM	pgr_dijkstra(
				'SELECT id, source::integer, target::integer, distance::double precision AS cost FROM topo_roads_noded', 
				(SELECT id_node FROM target), (SELECT id_node FROM farm),false) AS dijkstra
		LEFT JOIN
			topo_roads_noded
		ON
			(edge = id)
		ORDER BY
			seq
	)
SELECT *
FROM dijkstra
	
;

select * from tmp_topo_route_array;


DROP TABLE IF EXISTS tmp_topo_route_array_union;
CREATE TABLE tmp_topo_route_array_union AS
WITH
	lines AS (
		select end_vid, ST_Multi(ST_LineMerge(ST_Collect(geom))) as geom from tmp_topo_route_array
		group by end_vid
	)

select end_vid, ST_Length (geom) as length, geom from lines
;

select * from tmp_topo_route_array_union;


	SELECT DISTINCT array_agg(a.id_building)
	FROM (Select * from topo_targets where id_building > 0) as a
	JOIN (Select * from topo_targets where id_target = 1) as b
	ON ST_DWithin (a.geom, b.geom, 50000)
