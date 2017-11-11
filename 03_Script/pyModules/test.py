DROP TABLE IF EXISTS tmp_array_final;
CREATE TABLE tmp_array_final AS
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
		select array_agg(node_building2) as id_node from jrc_get_ids_arrays (1, 50000)

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
	),
	join_ids as (
		SELECT b.id_building2 as id_building, b.id_target2 as id_target, a.*
		FROM dijkstra a
		LEFT JOIN (select * from jrc_get_ids_arrays (1, 50000)) b
		ON (a.end_vid = b.node_building2)

	)
SELECT *
FROM join_ids
;


select * from tmp_array_final;

---
select * from tmp_array_union;


DROP TABLE IF EXISTS tmp_array_union;
CREATE TABLE tmp_array_union AS
WITH
	lines AS (
		select id_building, ST_Multi(ST_LineMerge(ST_Collect(geom))) as geom from tmp_array_final
		group by id_building
	)

select id_building, ST_Length (geom) as length, geom from lines
;

select * from tmp_array_union;
