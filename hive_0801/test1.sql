SELECT u.age AS age,
    avg(r.rate) AS avgrate
FROM t_rating r
    LEFT JOIN t_user u ON r.userid = u.userid
WHERE r.movieid = 2116
GROUP BY u.age;
