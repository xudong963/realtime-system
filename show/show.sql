SELECT count(*) AS c FROM sz_subway.dwd_subway_data

SELECT station AS column1, nums AS column2 FROM (SELECT * FROM sz_subway.dwd_in_station_rank  ORDER BY nums DESC) LIMIT 6

SELECT (count(*) / ((SELECT count(*) FROM sz_subway.dwd_out_station_data) + (SELECT count(*) FROM sz_subway.dwd_in_station_data))) AS value FROM sz_subway.dwd_out_station_data

SELECT (count(*) / ((SELECT count(*) FROM sz_subway.dwd_out_station_data) + (SELECT count(*) FROM sz_subway.dwd_in_station_data))) AS value FROM sz_subway.dwd_in_station_data

SELECT station AS column1, nums AS column2 FROM (SELECT * FROM sz_subway.dwd_out_station_rank  ORDER BY nums DESC) LIMIT 6