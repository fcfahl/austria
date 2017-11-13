CREATE SEQUENCE serial 
START 1
increment 1;


CREATE INDEX ON heads_manure (total);

-- Rank Heads 
DROP TABLE IF EXISTS heads_lsu_rank;
CREATE TABLE heads_lsu_rank AS
	SELECT mun_id AS mun_id1, total AS total_lsu, 0 AS rank1, 0 AS row1, '0'::text AS index1
	FROM heads_lsu
	ORDER BY mun_id ASC,  total DESC;

DROP TABLE IF EXISTS heads_manure_rank;
CREATE TABLE heads_manure_rank AS
	SELECT mun_id AS mun_id2, total AS total_manure, 0 AS rank2, 0 AS row2, '0'::text AS index2
	FROM heads_manure
	ORDER BY mun_id ASC,  total DESC;

DROP TABLE IF EXISTS heads_methane_rank;
CREATE TABLE heads_methane_rank AS
	SELECT mun_id AS mun_id3, total AS total_methane, 0 AS rank3, 0 AS row3, '0'::text AS index3
	FROM heads_methane
	ORDER BY mun_id ASC, total DESC;



-- add IDS for each farm 
ALTER SEQUENCE serial RESTART WITH 1;

ALTER TABLE heads_lsu_rank 
ADD COLUMN id1 int;
UPDATE heads_lsu_rank 
SET id1 = nextval('serial');

ALTER TABLE heads_manure_rank 
ADD COLUMN id2 int;
UPDATE heads_manure_rank 
SET id2 = nextval('serial');

ALTER TABLE heads_methane_rank 
ADD COLUMN id3 int;
UPDATE heads_methane_rank 
SET id3 = nextval('serial');



-- rank the farms according to municipal ID
WITH a AS 
(
    SELECT id1, mun_id1,
    row_number() OVER (ORDER BY mun_id1) AS row1,
    rank() OVER (ORDER BY mun_id1) AS rank1
    
    FROM heads_lsu_rank 
) 

UPDATE heads_lsu_rank AS b
   SET rank1 =  a.rank1, row1 = a.row1, index1 = concat (a.mun_id1, '_', (1 + a.row1 - a.rank1))
   FROM  a
   WHERE  a.id1 = b.id1;


WITH a AS 
(
    SELECT id2, mun_id2,
    row_number() OVER (ORDER BY mun_id2) AS row2,
    rank() OVER (ORDER BY mun_id2) AS rank2
    
    FROM heads_manure_rank 
) 

UPDATE heads_manure_rank AS b
   SET rank2 =  a.rank2, row2 = a.row2, index2 = concat (a.mun_id2, '_', (1 + a.row2 - a.rank2))
   FROM  a
   WHERE  a.id2 = b.id2;


WITH a AS 
(
    SELECT id3, mun_id3,
    row_number() OVER (ORDER BY mun_id3) AS row3,
    rank() OVER (ORDER BY mun_id3) AS rank3
    
    FROM heads_methane_rank 
) 

UPDATE heads_methane_rank AS b
   SET rank3 =  a.rank3, row3 = a.row3, index3 = concat (a.mun_id3, '_', (1 + a.row3 - a.rank3))
   FROM  a
   WHERE  a.id3 = b.id;

  

   


-- Join tables
DROP TABLE IF EXISTS farm_join;
CREATE TABLE farm_join AS
	SELECT a.*, b.total_lsu, c.total_manure, d.total_methane,
	b.total_lsu / a.total_area * 100 AS head_density
	FROM farm_buildings_union_centroids AS a
	LEFT JOIN heads_lsu_rank AS b ON a.index = b.index1
	LEFT JOIN heads_manure_rank AS c ON a.index = c.index2
	LEFT JOIN heads_methane_rank AS d ON a.index = d.index3
	;



SELECT * FROM farm_join  LIMIT 20 ;




