select
  p.p_partkey,
  p.p_mfgr
from
  cp.`tpch/part.parquet` p,
  cp.`tpch/partsupp.parquet` ps1
where
  p.p_partkey = ps1.ps_partkey
  and p.p_size = 41
  and ps1.ps_supplycost = (
    select
      min(ps.ps_supplycost)
    from
      cp.`tpch/partsupp.parquet` ps
    where
      p.p_partkey = ps.ps_partkey
  )
;



select
  p.p_partkey,
  p.p_mfgr
from
  cp.`tpch/part.parquet` p,
  cp.`tpch/partsupp.parquet` ps1
  join (
    select
      p2.p_partkey, 
      min(ps.ps_supplycost) as e1
    from
      cp.`tpch/partsupp.parquet` ps,
      cp.`tpch/part.parquet` p2
    where
      p2.p_partkey = ps.ps_partkey
    group by p2.p_partkey  
  )subq 
    on subq.partkey = p.partkey and ps1.supplycost = subq.e1
where
  p.p_partkey = ps1.ps_partkey and p.p_size = 41
  )
;