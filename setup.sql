CREATE TABLE nation  ( n_nationkey  INTEGER,
                            n_name       STRING,
                            n_regionkey  INTEGER,
                            n_comment    STRING);

LOAD DATA nation FROM '/home/jun/tpch_2_17_0/output/nation.tbl'

CREATE TABLE region  ( r_regionkey  INTEGER,
                            r_name       STRING,
                            r_comment    STRING);

LOAD DATA region FROM '/home/jun/tpch_2_17_0/output/region.tbl'

CREATE TABLE part  ( p_partkey     INTEGER,
                          p_name        STRING,
                          p_mfgr        STRING,
                          p_brand       STRING,
                          p_type        STRING,
                          p_size        INTEGER,
                          p_container   STRING,
                          p_retailprice FLOAT,
                          p_comment     STRING );

LOAD DATA part FROM '/home/jun/tpch_2_17_0/output/part.tbl'

CREATE TABLE supplier ( s_suppkey     INTEGER,
                             s_name        STRING,
                             s_address     STRING,
                             s_nationkey   INTEGER,
                             s_phone       STRING,
                             s_acctbal     FLOAT,
                             s_comment     STRING);

LOAD DATA supplier FROM '/home/jun/tpch_2_17_0/output/supplier.tbl'

CREATE TABLE partsupp ( ps_partkey     INTEGER,
                             ps_suppkey     INTEGER,
                             ps_availqty    INTEGER,
                             ps_supplycost  FLOAT,
                             ps_comment     STRING );

LOAD DATA partsupp FROM '/home/jun/tpch_2_17_0/output/partsupp.tbl'

CREATE TABLE customer ( c_custkey     INTEGER,
                             c_name        STRING,
                             c_address     STRING,
                             c_nationkey   INTEGER,
                             c_phone       STRING,
                             c_acctbal     FLOAT,
                             c_mktsegment  STRING,
                             c_comment     STRING);

LOAD DATA customer FROM '/home/jun/tpch_2_17_0/output/customer.tbl'

CREATE TABLE orders  ( o_orderkey       INTEGER,
                           o_custkey        INTEGER,
                           o_orderstatus    STRING,
                           o_totalprice     FLOAT,
                           o_orderdate      STRING,
                           o_orderpriority  STRING,
                           o_clerk          STRING,
                           o_shippriority   INTEGER,
                           o_comment        STRING);

LOAD DATA orders FROM '/home/jun/tpch_2_17_0/output/orders.tbl'

CREATE TABLE lineitem ( l_orderkey    INTEGER,
                             l_partkey     INTEGER,
                             l_suppkey     INTEGER,
                             l_linenumber  INTEGER,
                             l_quantity    FLOAT,
                             l_extendedprice  FLOAT,
                             l_discount    FLOAT,
                             l_tax         FLOAT,
                             l_returnflag  STRING,
                             l_linestatus  STRING,
                             l_shipdate    STRING,
                             l_commitdate  STRING,
                             l_receiptdate STRING,
                             l_shipinstruct STRING,
                             l_shipmode     STRING,
                             l_comment      STRING);

LOAD DATA lineitem FROM '/home/jun/tpch_2_17_0/output/lineitem.tbl'
