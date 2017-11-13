DROP TABLE IF EXISTS tmp_dijkstra_test;
CREATE TABLE tmp_dijkstra_test AS

WITh
    dijkstra as (
	SELECT
	    dijkstra.*, topo_roads_noded.geom
	FROM	pgr_dijkstra(
		'SELECT id, source::integer, target::integer, distance::double precision AS cost FROM topo_roads_noded',
		49719, array[33528,33237,340531,33194,326379],false) AS dijkstra
	LEFT JOIN
	    topo_roads_noded
	ON
	    (edge = id)
	ORDER BY
	    seq
),
line as (
SELECT c.end_vid, ST_Multi(ST_LineMerge(ST_Collect(c.geom))) AS geom
	FROM dijkstra AS c
	GROUP BY c.end_vid

)

SELECT d.end_vid, ST_Length (d.geom),  d.geom AS geom
	FROM line AS d


	  
;

select * from tmp_dijkstra_test;


