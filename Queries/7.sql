SELECT DISTINCT c_name 
FROM lineitem, orders, customer, nation, region
WHERE l_orderkey = o_orderkey AND o_custkey = c_custkey AND 
	c_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_regionkey = 3

