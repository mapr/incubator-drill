-- tpch4 using 1395599672 as a seed to the RNG
 select 1 from cp.`tpch/orders.parquet` o where exists (select 1 from cp.`tpch/lineitem.parquet` l where l.l_orderkey = o.o_orderkey);
 