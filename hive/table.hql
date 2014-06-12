set hive.exec.dynamic.partition.=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.optimize.bucketmapjoin=true;
set mapred.child.java.opts = -Xmx1024m;
