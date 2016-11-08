ElasticSearch Hadoop Integration

Overview:
      Today we have logstash and few other tools to move entire logs to ElasticSearch cluster by applying filters. The elasticsearch cluster data size is growing day by day.
Finally we will end up with disk space issue , so ES cluster should be expanded by adding more nodes. As part of this ES Hadoop Integration we will utilizing the same 
ES Cluster to store only the Computed Result ( Key , Value Pairs ). 

Three Components:
1) Client Agent ( to move data to Central Aggr )
2) Central Aggr  (Who collectes all nodes data )
3) Spark Job  ( Polls Central Aggr and apply computation then directly writes to ES )

