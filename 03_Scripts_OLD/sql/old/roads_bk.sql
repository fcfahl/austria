

SELECT DISTINCT value FROM osm_roads;

-- residential areas
DROP TABLE IF EXISTS osm_residential;
CREATE TABLE osm_residential AS 
	SELECT * 
	FROM osm_landuse 
	WHERE value = 'residential';


-- Farm roads (dissolve and crop)
DROP TABLE IF EXISTS farm_roads;
CREATE TABLE farm_roads AS 
	SELECT ST_Union(a.geom) as geom
	FROM osm_roads AS a, adm AS b
	WHERE (a.value = 'primary' 
		OR a.value = 'secondary' 
		OR a.value = 'tertiary'
		OR a.value = 'motorway'
		OR a.value = 'motorway_link'
		OR a.value = 'unclassified'
		OR a.value = 'service'
		OR a.value = 'residential'
		OR a.value = 'track')
	AND	ST_Intersects(a.geom, b.geom)		
	;

-- Snap farms to roads
DROP TABLE IF EXISTS farm_snap;
CREATE TABLE farm_snap AS 
	SELECT a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane, ST_Closestpoint(ST_Collect(b.geom), a.geom) AS geom 
	FROM farm_join a, farm_roads b
	GROUP BY a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane, a.geom;

	
-- Extract roads inside buffer
DROP TABLE IF EXISTS farm_roads_30531_62;
CREATE TABLE farm_roads_30531_62 AS 
	SELECT a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane,
		ST_Union(ST_SnapToGrid(ST_Intersection(ST_Buffer(a.geom, 5000), b.geom),0.001)) AS geom		
	FROM farm_snap AS a, farm_roads AS b
	WHERE a.index = '30531_62'
	GROUP BY a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane
	;

SELECT *, ST_GeometryType(geom) FROM farm_roads_30531_62;


DROP TABLE IF EXISTS farm_roads_30531_63;
CREATE TABLE farm_roads_30531_63 AS 
	SELECT a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane,
		ST_Union(ST_SnapToGrid(ST_Intersection(ST_Buffer(a.geom, 5000), b.geom),0.001)) AS geom		
	FROM farm_snap AS a, farm_roads AS b
	WHERE a.index = '30531_63'
	GROUP BY a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane
	;

SELECT *, ST_GeometryType(geom) FROM farm_roads_30531_63;

DROP TABLE IF EXISTS farm_roads_30531_64;
CREATE TABLE farm_roads_30531_64 AS 
	SELECT a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane,
		ST_Union(ST_SnapToGrid(ST_Intersection(ST_Buffer(a.geom, 5000), b.geom),0.001)) AS geom		
	FROM farm_snap AS a, farm_roads AS b
	WHERE a.index = '30531_64'
	GROUP BY a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane
	;

SELECT *, ST_GeometryType(geom) FROM farm_roads_30531_64;


DROP TABLE IF EXISTS farm_roads_30531_65;
CREATE TABLE farm_roads_30531_65 AS 
	SELECT a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane,
		ST_Union(ST_SnapToGrid(ST_Intersection(ST_Buffer(a.geom, 5000), b.geom),0.001)) AS geom		
	FROM farm_snap AS a, farm_roads AS b
	WHERE a.index = '30531_65'
	GROUP BY a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane
	;

SELECT *, ST_GeometryType(geom) FROM farm_roads_30531_65;



-- Find optimal location
DROP TABLE IF EXISTS plant_1;
CREATE TABLE plant_1 AS 
	SELECT (a.total_lsu + b.total_lsu) as total, e.geom	as geom
	FROM farm_roads_30531_62 AS a, farm_roads_30531_62 AS b,
		farm_roads_30531_64 AS c, farm_roads_30531_65 AS d,
		farm_roads AS e

			;

SELECT *, ST_GeometryType(geom) FROM plant_1;


DROP TABLE IF EXISTS farm_buildings_union;
CREATE TABLE farm_buildings_union AS
	SELECT id_cluster, id_mun, ST_Multi(ST_Union(geom)) as geom, sum(ST_Area(geom)) AS total_area
	FROM farm_buildings_crop 
	GROUP BY id_cluster, id_mun
	ORDER BY total_area;


	
-- Create topology
CREATE EXTENSION postgis_topology;
SELECT topology.CreateTopology('farm_roads_buffer_topo', 3035);
SELECT topology.AddTopoGeometryColumn('farm_roads_buffer_topo', 'public', 'farm_roads_buffer', 'topo_geom', 'LINESTRING');
UPDATE farm_roads_buffer SET topo_geom = topology.toTopoGeom(wkb_geometry, 'farm_roads_buffer_topo', 1, 1.0);


DO $$DECLARE r record;
BEGIN
  FOR r IN SELECT * FROM farm_roads_buffer LOOP
    BEGIN
      UPDATE farm_roads_buffer SET topo_geom = topology.toTopoGeom(geom, 'farm_roads_buffer_topo', 1, 1.0)
      WHERE id = r.id;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE WARNING 'Loading of record % failed: %', r.id, SQLERRM;
    END;
  END LOOP;
END$$;


SELECT r.lib_off, r.ogc_fid, e.geom
FROM farm_roads_buffer_topo.edge_data e,
     farm_roads_buffer_topo.relation rel,
     farm_roads_buffer_topo.node r
WHERE e.edge_id = rel.element_id
  AND rel.topogeo_id = (r.topo_geom).id



DROP TABLE IF EXISTS farm_roads_buffer_split;
CREATE TABLE farm_roads_buffer_split AS 
	SELECT a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane,
		ST_Intersection (ST_Buffer(a.geom, 5000), b.geom) AS geom		
	FROM farm_snap AS a, farm_roads AS b
	WHERE a.index = '30531_62'
	;


SELECT *  FROM farm_roads_buffer;

SELECT GeometryType(geom) FROM farm_roads_buffer;