SELECT SUM(c_acctbal), c_name 
FROM customer, orders 
WHERE c_custkey = 121 AND c_custkey = o_custkey

