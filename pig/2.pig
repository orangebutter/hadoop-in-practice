data2= LOAD '/user/hdfs/base_station_data/output150-09-17_3/part-00000' USING PigStorage('|') AS (imsi:chararray,loc:chararray,time:chararray,timelength:double);

top1 = FOREACH ( GROUP data2 BY (imsi,loc) )
{
	sorted = ORDER data2 BY timelength DESC;
	top = LIMIT sorted 2;
	GENERATE FLATTEN(top);
}

top_time = FOREACH ( GROUP data2 BY (imsi,time) ) GENERATE FLATTEN(group) AS (imsi,time),MAX(data2.timelength) AS maxtime;
