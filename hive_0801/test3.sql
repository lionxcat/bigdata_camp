-- 3,的平均影评分，展示电影名和平均影评分
SELECT collect_set(m.moviename) [0] AS moviename,
    AVG(r.rate) AS avgrate
FROM t_rating r
    JOIN t_movie m ON r.movieid = m.movieid
WHERE r.movieid IN (
        SELECT movieid
        FROM (
                -- 2.所给出最高分的10部电影
                SELECT r.movieid AS movieid,
                    r.rate AS rate
                FROM t_rating r
                    JOIN (
                        -- 1.影评次数最多的女士
                        SELECT u.userid AS userid,
                            COUNT(1) AS rate_count
                        FROM t_rating r
                            LEFT JOIN t_user u ON r.userid = u.userid
                        WHERE u.sex = 'F'
                        GROUP BY u.userid
                        ORDER BY rate_count DESC
                        LIMIT 1
                    ) AS f ON r.userid = f.userid
                ORDER BY rate DESC
                LIMIT 10
            ) AS t10
    )
GROUP BY r.movieid;
