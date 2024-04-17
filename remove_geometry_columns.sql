ALTER TABLE stations
ADD COLUMN longitude FLOAT;

ALTER TABLE stations
ADD COLUMN latitude FLOAT;

UPDATE stations
SET longitude = ST_X(geom_4326)
WHERE 1=1;

UPDATE stations
SET latitude = ST_Y(geom_4326)
WHERE 1=1;

ALTER TABLE stations
DROP COLUMN geom_4326;

ALTER TABLE stations
DROP COLUMN geom_25830;
