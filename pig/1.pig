Example usage:
--ע��piggybank��
REGISTER /usr/local/pig/contrib/piggybank/java/piggybank.jar
REGISTER /usr/local/pig/contrib/piggybank/java/lib/joda-time-2.3.jar

--����customFormatToISO����
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
--��lbs��ȡ����
lbsdata= LOAD '/user/hdfs/lbs.txt' USING PigStorage('|') AS (imsi:chararray,time:chararray,loc:chararray);

--lbsdata�����е����ڻ���iso��ʽ
toISO = FOREACH lbsdata GENERATE imsi,CustomFormatToISO( SUBSTRING(time,0,13),'YYYY-MM-DD HH') AS time:chararray,loc;
--����imsi��toISO���ݷ���
grp = GROUP toISO by imsi;

--ע��datafu��
REGISTER /usr/local/pig/contrib/datafu/datafu-1.2.0-SNAPSHOT.jar
--ʹ������ɷ���
DEFINE MarkovPairs datafu.pig.stats.MarkovPairs();

--ʹ������ɷ��������ڵ�������¼����һ����¼
	--toISO����time����
	--������ɷ�����ȡ��������
	--��pair���ݱ�ƽ��
pairs = FOREACH grp { 
	sorted = ORDER toISO By time;
	pair = MarkovPairs(sorted);
	
	GENERATE FLATTEN(pair) AS ( data:tuple(imsi,time,loc),next:tuple(imsi,time,loc) );
	}
	
	
--�����ݱ�ƽ
prj = FOREACH pairs GENERATE data.imsi AS imsi,data.time AS time,next.time AS next_time,data.loc AS loc,next.loc AS next_loc; 

DEFINE ISODaysBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISODaysBetween();

flt = FILTER prj BY ISODaysBetween(next_time,time) < 12L;
--����locͳ������
total_count = FOREACH ( GROUP flt BY loc ) GENERATE group AS loc,COUNT(flt) AS total;
--ͳ��loc��next_loc���ܺ�

pairs_count = FOREACH ( GROUP flt BY (loc,next_loc) ) GENERATE FLATTEN(group) AS (loc,next_loc),COUNT(flt) AS cnt;

jnd = JOIN pairs_count BY loc, total_count BY loc USING 'replicated';

prob = FOREACH jnd GENERATE pairs_count::loc AS loc,pairs_count::next_loc AS next_loc, (double)cnt/ (double)total AS probability;

top3 = FOREACH ( GROUP prob BY loc )
{
	sorted = ORDER prob BY probability DESC;
	top = LIMIT sorted 3;
	GENERATE FLATTEN(top);
}

--��pig���ڽ������
DEFINE ISOToHour org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToHour();
flt_time = FOREACH flt GENERATE imsi,CONISOToHour(time) as time_hour ,ISOToHour(next_time) AS next_time_hour, loc, next_loc; 

pairs_count_time = FOREACH ( GROUP flt_time BY (loc,next_loc) ) GENERATE FLATTEN(group) AS (loc,next_loc),COUNT(flt) AS cnt;
