SELECT DISTINCT c_address
FROM customer, nation, region
WHERE c_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_regionkey = 2

