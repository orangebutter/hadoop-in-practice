Example usage:
--注册piggybank包
REGISTER /usr/local/pig/contrib/piggybank/java/piggybank.jar
REGISTER /usr/local/pig/contrib/piggybank/java/lib/joda-time-2.3.jar

--定义customFormatToISO函数
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
--从lbs读取数据
lbsdata= LOAD '/user/hdfs/lbs.txt' USING PigStorage('|') AS (imsi:chararray,time:chararray,loc:chararray);

--lbsdata数据中的日期换成iso格式
toISO = FOREACH lbsdata GENERATE imsi,CustomFormatToISO( SUBSTRING(time,0,13),'YYYY-MM-DD HH') AS time:chararray,loc;
--按照imsi对toISO数据分组
grp = GROUP toISO by imsi;

--注册datafu包
REGISTER /usr/local/pig/contrib/datafu/datafu-1.2.0-SNAPSHOT.jar
--使用马尔可夫函数
DEFINE MarkovPairs datafu.pig.stats.MarkovPairs();

--使用马尔可夫函数将相邻的两条记录放在一条记录
	--toISO按照time排序
	--用马尔可夫链获取数据链条
	--将pair数据扁平化
pairs = FOREACH grp { 
	sorted = ORDER toISO By time;
	pair = MarkovPairs(sorted);
	
	GENERATE FLATTEN(pair) AS ( data:tuple(imsi,time,loc),next:tuple(imsi,time,loc) );
	}
	
	
--将数据扁平
prj = FOREACH pairs GENERATE data.imsi AS imsi,data.time AS time,next.time AS next_time,data.loc AS loc,next.loc AS next_loc; 

DEFINE ISODaysBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISODaysBetween();

flt = FILTER prj BY ISODaysBetween(next_time,time) < 12L;
--按照loc统计总数
total_count = FOREACH ( GROUP flt BY loc ) GENERATE group AS loc,COUNT(flt) AS total;
--统计loc，next_loc的总和

pairs_count = FOREACH ( GROUP flt BY (loc,next_loc) ) GENERATE FLATTEN(group) AS (loc,next_loc),COUNT(flt) AS cnt;

jnd = JOIN pairs_count BY loc, total_count BY loc USING 'replicated';

prob = FOREACH jnd GENERATE pairs_count::loc AS loc,pairs_count::next_loc AS next_loc, (double)cnt/ (double)total AS probability;

top3 = FOREACH ( GROUP prob BY loc )
{
	sorted = ORDER prob BY probability DESC;
	top = LIMIT sorted 3;
	GENERATE FLATTEN(top);
}

--找pig日期解决方案
DEFINE ISOToHour org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToHour();
flt_time = FOREACH flt GENERATE imsi,CONISOToHour(time) as time_hour ,ISOToHour(next_time) AS next_time_hour, loc, next_loc; 

pairs_count_time = FOREACH ( GROUP flt_time BY (loc,next_loc) ) GENERATE FLATTEN(group) AS (loc,next_loc),COUNT(flt) AS cnt;
