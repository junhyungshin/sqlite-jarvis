SELECT SUM(l_extendedprice * l_discount * (1.0-l_tax))
FROM lineitem
WHERE l_orderkey > 10 AND l_orderkey < 51

