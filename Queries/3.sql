SELECT l_receiptdate
FROM lineitem, orders, customer
WHERE	l_orderkey = o_orderkey AND o_custkey = c_custkey AND
	o_orderkey > 10 AND o_orderkey < 21

