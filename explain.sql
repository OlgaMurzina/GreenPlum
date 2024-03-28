-- 1) 
/* неэффективный запрос */
SELECT SUM(re.sale_price)
FROM homework_3.real_estate re 
WHERE date_trunc('year',"date") = '2009-01-01'

/* делаем его эффективным */
EXPLAIN analyze SELECT SUM(re.sale_price)
FROM homework_3.real_estate re 
WHERE date = '2009-01-01';

-- 2)
/* неэффективный запрос */
SELECT s.track_id,
	a.track_name,
	track_album_id,
	track_popularity
FROM homework_3.spotify_songs_more_artists a 
JOIN homework_3.spotify_songs s 
	ON a.track_id = s.track_id
WHERE track_artist = 'Sia'
UNION ALL
SELECT s.track_id,
       a.track_name,
       track_album_id,
       track_popularity
FROM homework_3.spotify_songs_more_artists a
JOIN homework_3.spotify_songs s 
	ON a.track_id = s.track_id
WHERE duration_ms::int between 299900 and 300827

/* делаем его эффективным */
explain analyze 
with a as(
SELECT s.track_id,
	a.track_name,
	track_album_id,
	track_popularity
FROM homework_3.spotify_songs_more_artists a 
JOIN homework_3.spotify_songs s 
	ON a.track_id = s.track_id
WHERE track_artist = 'Sia' or duration_ms::int between 299900 and 300827
)

select count(*)
from a

