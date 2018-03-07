SELECT SUM(l_discount) 
FROM customer, orders, lineitem
WHERE c_custkey = o_custkey AND o_orderkey = l_orderkey AND c_custkey = 222

