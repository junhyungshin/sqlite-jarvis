SELECT SUM(ps_supplycost), s_suppkey
FROM part, supplier, partsupp 
WHERE p_partkey = 10 AND p_partkey = ps_partkey AND s_suppkey = ps_suppkey
GROUP BY s_suppkey

