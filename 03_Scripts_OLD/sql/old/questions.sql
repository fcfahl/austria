-- How many farms ?
SELECT count(*) FROM farm_snap;


-- How many municipalities ?
SELECT count(*) FROM 
(SELECT DISTINCT mun_id FROM adm) as a;


-- How to create spatial index?
CREATE INDEX idx_farm_roads_segment
ON farm_roads_segment
USING gist(geom);

Declare

SELECT i,j,k FROM generate_series(1, (SELECT count(*) FROM farm_snap)) As i 
    CROSS JOIN generate_series(1,5) As j  
        CROSS JOIN generate_series(1,5) As k 
        ORDER BY k,i,j;


CREATE TABLE employees (
 employee_id serial PRIMARY KEY,
 full_name VARCHAR NOT NULL,
 manager_id INT
);

INSERT INTO employees (
 employee_id,
 full_name,
 manager_id
)
VALUES
 (1, 'Michael North', NULL),
 (2, 'Megan Berry', 1),
 (3, 'Sarah Berry', 1),
 (4, 'Zoe Black', 1),
 (5, 'Tim James', 1),
 (6, 'Bella Tucker', 2),
 (7, 'Ryan Metcalfe', 2),
 (8, 'Max Mills', 2),
 (9, 'Benjamin Glover', 2),
 (10, 'Carolyn Henderson', 3),
 (11, 'Nicola Kelly', 3),
 (12, 'Alexandra Climo', 3),
 (13, 'Dominic King', 3),
 (14, 'Leonard Gray', 4),
 (15, 'Eric Rampling', 4),
 (16, 'Piers Paige', 7),
 (17, 'Ryan Henderson', 7),
 (18, 'Frank Tucker', 8),
 (19, 'Nathan Ferguson', 8),
 (20, 'Kevin Rampling', 8);



WITH RECURSIVE subordinates AS (
 SELECT
 employee_id,
 manager_id,
 full_name
 FROM
 employees
 WHERE
 employee_id = 2
 UNION ALL
 SELECT
 e.employee_id,
 e.manager_id,
 e.full_name
 FROM
 employees e
 INNER JOIN subordinates s ON s.employee_id = e.manager_id
) SELECT
 *
FROM
 subordinates;