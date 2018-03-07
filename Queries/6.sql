SELECT DISTINCT n_name
FROM nation, customer, orders
WHERE o_orderkey > 10 AND o_orderkey < 10000 AND o_custkey=c_custkey AND
	n_nationkey=c_nationkey

