SELECT m.moviename AS name,
    ur.avg_rate AS avgrate,
    ur.rate_count AS total
FROM (
        SELECT r.movieid as movieid,
            COUNT(r.userid) AS rate_count,
            AVG(r.rate) AS avg_rate
        FROM t_rating r
            LEFT JOIN t_user u ON r.userid = u.userid
        WHERE u.sex = 'M'
        GROUP BY r.movieid
    ) ur
    LEFT JOIN t_movie m ON ur.movieid = m.movieid
WHERE ur.rate_count > 50
ORDER BY avgrate DESC
LIMIT 10;
