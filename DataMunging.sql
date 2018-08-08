SELECT COUNT(*) FROM trainFraud
SELECT COUNT(*) FROM testFraud


184903890

SELECT DISTINCT ip FROM trainFraud--277396
SELECT DISTINCT app FROM trainFraud--706

SELECT DISTINCT device FROM trainFraud--3000
SELECT DISTINCT os FROM trainFraud --800
SELECT DISTINCT channel FROM trainFraud--202
SELECT DISTINCT attributed_time FROM trainFraud where attributed_time is not null
SELECT COUNT(attributed_time) FROM trainFraud where attributed_time is not null


SELECT DISTINCT click_time FROM trainFraud where click_time is not null
SELECT COUNT(click_time) FROM trainFraud where click_time is not null
SELECT DISTINCT is_attributed FROM trainFraud






ip
app
device
os
channel
click_time
attributed_time
is_attributed

SELECt TOP 1 * FROM trainFraud

SELECT TOP 10 CAST(DATEPART(HOUR, Click_Time)AS INT)  from trainFraud


SELECT TOP 10 CAST(DATENAME(WEEKDAY, Click_Time) AS VARCHAR(20))  from trainFraud

/*
ALTER TABLE trainFraud ADD ClickHour INT;
ALTER TABLE trainFraud ADD ClickDayName VARCHAR(20);*/


UPDATE trainFraud SET ClickHour = CAST(DATEPART(HOUR, Click_Time)AS INT)
,ClickDayName  =CAST(DATENAME(WEEKDAY, Click_Time) AS VARCHAR(20))d


DBCC SHRINKDATABASE ('dev',5) WITH NO_INFOMSGS


USE DEV;  
GO  
DBCC SHRINKFILE (dev_log, 5);  
GO  

/*
SELECT 
ip
,app
,device
,os
,channel
,click_time
,attributed_time
,is_attributed
,ClickHour = CAST(DATEPART(HOUR, Click_Time)AS INT)
,ClickDayName  =CAST(DATENAME(WEEKDAY, Click_Time) AS VARCHAR(20))
INTO CleantrainFraud
FROM trainFraud
*/

SELECT top 10 *,FLOOR(RAND()*(5-1)+1) AS Rnd FROM CleantrainFraud

SELECT TOP 10 *,1+ABS(CHECKSUM(NewId())) % 4 AS Rnd FROM CleantrainFraud


--SELECT *,1+ABS(CHECKSUM(NewId())) % 4 AS Rnd INTO trainFraud FROM CleantrainFraud


SELECT FLOOR(RAND()*(5-1)+1);

ALTER TABLE trainFraud ADD ID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED;

CREATE NONCLUSTERED INDEX NCI_Rnd ON trainFraud(Rnd);


SELECT COUNT(1),Rnd,is_attributed FROM trainFraud
GROUP BY Rnd,is_attributed
ORDER BY Rnd


/*DATA analysis begins*/
SELECT COUNT(1), ClickdayName FROM trainFraud
WHERE is_Attributed=1 AND Rnd=4
GROUP BY ClickDayName
--Clearly Monday has very few chances of downloads and only Tue,Wed,Thur as only days when downloads happened

SELECT COUNT(1), ClickdayName FROM trainFraud
WHERE is_Attributed=0 AND Rnd=4
GROUP BY ClickDayName


--Analysis for click Hours
SELECT COUNT(1), ClickHour FROM trainFraud
WHERE is_Attributed=1 AND Rnd=4
GROUP BY ClickHour


SELECT COUNT(1), ClickHour FROM trainFraud
WHERE is_Attributed=0 AND Rnd=4
GROUP BY ClickHour

/*Lets build the test dataset at par with Train*/
Select TOP 1 * FROM CleantestFraud
/*
SELECT 
click_id,
ip
,app
,device
,os
,channel
,click_time
,ClickHour = CAST(DATEPART(HOUR, Click_Time)AS INT)
,ClickDayName  =CAST(DATENAME(WEEKDAY, Click_Time) AS VARCHAR(20))
INTO CleantestFraud
FROM testFraud
*/


------------------------
Select TOP 1 * FROM CleantestFraud
select top 100 * from dfFinal (NOLOCK) where is_attributed = 1
select count(*),is_attributed from dfFinal (NOLOCK)
GROUP BY is_attributed

is_attributed = 1
SELECT click_id,is_attributed FROM dfFinal;
--ALTER TABLE dfFinal ALTER COLUMN is_attributed INT;
-------------------------
/*
train[, UsrappCount:=.N, by=list(ip,app,device,os)]
train[, UsrappNewness:=1:.N, by=list(ip,app,device,os)]
train[, UsrCount:=.N, by=list(ip,device,os)]
train[, UsrNewness:=1:.N, by=list(ip,device,os)]
categorical_features = c("app", "device", "os", "channel", "hour")
*/



SELECT ip,device,os FROM trainFraud WHERE Rnd=4
GROUP BY ip,device,os
ORDER BY ip,device,os



--SELECT * INTO trainFraudRnd4 FROM trainFraud WHERE Rnd=4

SELECT ip,device,os FROM trainFraud WHERE Rnd=4
GROUP BY ip,device,os
ORDER BY ip,device,t.

SELECT t.ip,t.app,t.device,t.os,t.channel
,UserNewness = ROW_NUMBER() OVER (PARTITION BY t.ip,t.device,t.os ORDER BY t.click_time)
,UserCount  = (SELECT COUNT(*) FROM trainFraudRnd4 tf where tf.ip=t.ip AND tf.device=t.device AND tf.os=t.os)
,UserAppNewness= ROW_NUMBER() OVER (PARTITION BY t.ip,t.app,t.device,t.os ORDER BY t.click_time)
,UserAppCount  = (SELECT COUNT(*) FROM trainFraudRnd4 tf2 where tf2.ip=t.ip AND tf2.app = t.app AND tf2.device=t.device AND tf2.os=t.os)
,t.click_time,t.is_attributed,t.ClickHour
INTO trainFraudNew
FROM trainFraudRnd4 t






-- prepare the test data
SELECT t.click_id,t.ip,t.app,t.device,t.os,t.channel
,UserNewness = ROW_NUMBER() OVER (PARTITION BY t.ip,t.device,t.os ORDER BY t.click_time)
,UserCount  = (SELECT COUNT(*) FROM CleantestFraud tf where tf.ip=t.ip AND tf.device=t.device AND tf.os=t.os)
,UserAppNewness= ROW_NUMBER() OVER (PARTITION BY t.ip,t.app,t.device,t.os ORDER BY t.click_time)
,UserAppCount  = (SELECT COUNT(*) FROM CleantestFraud tf2 where tf2.ip=t.ip AND tf2.app = t.app AND tf2.device=t.device AND tf2.os=t.os)
,t.click_time,t.ClickHour
INTO testFraudNew
FROM CleantestFraud t 


--Use Queries
SELECT ip,app,device,os,channel,ClickHour,UserNewness,UserCount,UserAppNewness,UserAppCount,is_attributed FROM trainFraudNew
SELECT TOP 1 click_id,ip,app,device,os,channel,ClickHour,UserNewness,UserCount,UserAppNewness,UserAppCount FROM testFraudNew ORDER BY click_id asc


SELECT TOP 1 *  FROM dfFinal order by click_id

SELECT click_id,is_attributed FROM dfFinal



---- 0415 Attempt 3

SELECT DISTINCT(CAST (Click_time AS DATE)) AS  [Date],COUNT(*) FROM trainFraud
GROUP BY (CAST (Click_time AS DATE))
2017-11-06	9308568
2017-11-07	59633310
2017-11-08	62945075
2017-11-09	53016937
--Lets pick for furher analysis the latest 2017-11-09 data which is about 53 million records.

SELECT TOP 1 * FROM trainFraud WHERE  (CAST (Click_time AS DATE)) = CAST('2017-11-09' AS DATE)


SELECT * INTO trainSubSet
FROM trainFraud t WHERE  CAST (Click_time AS DATE) = CAST('2017-11-09' AS DATE) AND ClickHour IN (4,5,6,9,10,11,13,14,15)

--TRAIN FEATURE Engineering
SELECT t.app,t.device,t.os,t.channel,t.ClickHour
,ipHourAppCount = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.ip=t.ip AND t1.ClickHour=t.ClickHour AND t1.app = t.app)
,ipHourDeviceCount = (SELECT COUNT(*) FROM trainSubSet t2 WHERE t2.ip=t.ip AND t2.ClickHour=t.ClickHour AND t2.device = t.device)
,ipHourOSCount = (SELECT COUNT(*) FROM trainSubSet t3 WHERE t3.ip=t.ip AND t3.ClickHour=t.ClickHour AND t3.os = t.os)
,ipHourChannelCount = (SELECT COUNT(*) FROM trainSubSet t4 WHERE t4.ip=t.ip AND t4.ClickHour=t.ClickHour AND t4.channel = t.channel)
,is_attributed
INTO trainFeaEng
FROM trainSubSet t
--(30840433 rows affected)


-- We find that only these hours are there in the test data hence we will use only these hours in the train sample dataset as well
SELECT distinct ClickHour FROM CleantestFraud
--(4,5,6,9,10,11,13,14,15)

SELECT distinct app FROM CleantestFraud


--TEST FEATURE Engineering
SELECT t.click_id,t.app,t.device,t.os,t.channel,t.ClickHour
,ipHourAppCount = (SELECT COUNT(*) FROM CleantestFraud t1 WHERE t1.ip=t.ip AND t1.ClickHour=t.ClickHour AND t1.app = t.app)
,ipHourDeviceCount = (SELECT COUNT(*) FROM CleantestFraud t2 WHERE  t2.ip=t.ip AND t2.ClickHour=t.ClickHour AND t2.device = t.device)
,ipHourOSCount = (SELECT COUNT(*) FROM CleantestFraud t3 WHERE t3.ip=t.ip AND t3.ClickHour=t.ClickHour AND t3.os = t.os)
,ipHourChannelCount = (SELECT COUNT(*) FROM CleantestFraud t4 WHERE t4.ip=t.ip AND t4.ClickHour=t.ClickHour AND t4.channel = t.channel)
INTO testFeaEng
FROM CleantestFraud t 
--(18790469 rows affected)

/*
-- Total Samples 30840433
-- Negative Samples 30765531
--Positive Samples 74902
--Calculating scale_position_weight for lightgbm as this is an unbalanced class with no. of positives is very less compared to -ives
scale_pos_weight = 100 - ( [**number of positive samples** / **total samples** ] * 100 )
scale_pos_weight = 100 - ( [ 74902 / 30840433 ] * 100 )  = 100-0.2428 
scale_pos_weight = 99.7572

*/

---Attempt 0416
SELECT * INTO trainSubSet 
FROM trainFraud t WHERE  CAST (Click_time AS DATE) IN ( CAST('2017-11-08' AS DATE),CAST('2017-11-09' AS DATE)) 
AND ClickHour IN (4,5,6,9,10,11,13,14,15)

Select distinct attributed_time from trainSubSet where attributed_time IS NOT NULL



SELECT t.app,t.device,t.os,t.channel,t.ClickHour
,ipHourAppCount = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.ip=t.ip AND t1.ClickHour=t.ClickHour AND t1.app = t.app)
,ipHourDeviceCount = (SELECT COUNT(*) FROM trainSubSet t2 WHERE t2.ip=t.ip AND t2.ClickHour=t.ClickHour AND t2.device = t.device)
,ipHourOSCount = (SELECT COUNT(*) FROM trainSubSet t3 WHERE t3.ip=t.ip AND t3.ClickHour=t.ClickHour AND t3.os = t.os)
,ipHourChannelCount = (SELECT COUNT(*) FROM trainSubSet t4 WHERE t4.ip=t.ip AND t4.ClickHour=t.ClickHour AND t4.channel = t.channel)
,is_attributed
INTO trainFeaEng
FROM trainSubSet t


/*Attempt 0419*/

SELECT top 10 * FROM trainFeaEng
SELECT DISTINCT Channel FROM testFeaEng WHERE Channel NOT IN (
SELECT DISTINCT Channel from trainFeaEng )
169
172



SELECT DISTINCT OS FROM testFeaEng WHERE Channel NOT IN (
SELECT DISTINCT OS from trainFeaEng )
--280 OS is missing

SELECT DISTINCT app FROM testFeaEng WHERE Channel NOT IN (
SELECT DISTINCT app from trainFeaEng )
--84 app missing
SELECT DISTINCT device FROM testFeaEng WHERE Channel NOT IN (
SELECT DISTINCT device from trainFeaEng )
-- 18 devices missing

--Checking the same in trainSubSet

DROP TABLE #trainSubSet 

SELECT * INTO #trainSubSet 
FROM trainFraud t WHERE  CAST (Click_time AS DATE) IN (CAST('2017-11-09' AS DATE),CAST('2017-11-08' AS DATE))
AND ClickHour IN (4,5,6,9,10,11,13,14,15)

SELECT DISTINCT Channel FROM testFeaEng WHERE Channel NOT IN (
SELECT DISTINCT Channel from #trainSubSet) WHERE ClickHour IN (4,5,6,9,10,11,13,14,15))--NOT MISSING
--2 OS is missing
SELECT DISTINCT OS FROM testFeaEng WHERE OS NOT IN (
SELECT DISTINCT OS from #trainSubSet) WHERE ClickHour IN (4,5,6,9,10,11,13,14,15))--80 MISSING
--280 OS is missing
SELECT DISTINCT app FROM testFeaEng WHERE app NOT IN (
SELECT DISTINCT app from #trainSubSet) WHERE ClickHour IN (4,5,6,9,10,11,13,14,15))-- 33 MISSING
--84 app missing
SELECT DISTINCT device FROM testFeaEng WHERE device NOT IN (
SELECT DISTINCT device from #trainSubSet) --324 MISSING values
-- 18 devices missing



--WITH the data analysis to minimize the missig data in train dataset
--going with below condition
--(CAST('2017-11-09' AS DATE),CAST('2017-11-08' AS DATE)) AND ClickHour IN (4,5,6,9,10,11,13,14,15)

SELECT CAST (Click_time AS DATE) AS [Date],COUNT(*)
FROM trainFraud
WHERE ClickHour IN (4,5,6,9,10,11,13,14,15)
GROUP BY CAST (Click_time AS DATE)


SELECT * INTO trainSubSet 
FROM trainFraud t WHERE  CAST (Click_time AS DATE) =CAST('2017-11-09' AS DATE)
AND ClickHour IN (4,5,6,9,10,11,13,14,15)



SELECT COUNT(distinct ip,channel)  FROM trainSubSet


/*Analysis Complete Building the Test and train Dataset*/



SELECT t.app,t.device,t.os,t.channel,t.ClickHour
,ipHourAppCount = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.ip=t.ip AND t1.ClickHour=t.ClickHour AND t1.app = t.app)
,ipHourDeviceCount = (SELECT COUNT(*) FROM trainSubSet t2 WHERE t2.ip=t.ip AND t2.ClickHour=t.ClickHour AND t2.device = t.device)
,ipHourOSCount = (SELECT COUNT(*) FROM trainSubSet t3 WHERE t3.ip=t.ip AND t3.ClickHour=t.ClickHour AND t3.os = t.os)
,ipHourChannelCount = (SELECT COUNT(*) FROM trainSubSet t4 WHERE t4.ip=t.ip AND t4.ClickHour=t.ClickHour AND t4.channel = t.channel)
--,ipApp = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.ip=t.ip AND t1.app = t.app)
--,ipAppOS = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.ip=t.ip AND t1.app = t.app AND t1.OS = t.OS)
--,ipDevice = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.ip=t.ip AND t1.device = t.device)
--,AppOS = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.app=t.app AND t1.OS = t.OS)
--,AppOSChannel = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.app=t.app AND t1.OS = t.OS AND t1.channel = t.channel)
--,ipChannel = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.ip=t.ip AND t1.channel = t.channel)
,is_attributed
INTO trainFeaEng
FROM trainSubSet t


/*Build Test*/

SELECT t.click_id,t.app,t.device,t.os,t.channel,t.ClickHour
,ipHourAppCount = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.ip=t.ip AND t1.ClickHour=t.ClickHour AND t1.app = t.app)
,ipHourDeviceCount = (SELECT COUNT(*) FROM trainSubSet t2 WHERE t2.ip=t.ip AND t2.ClickHour=t.ClickHour AND t2.device = t.device)
,ipHourOSCount = (SELECT COUNT(*) FROM trainSubSet t3 WHERE t3.ip=t.ip AND t3.ClickHour=t.ClickHour AND t3.os = t.os)
,ipHourChannelCount = (SELECT COUNT(*) FROM trainSubSet t4 WHERE t4.ip=t.ip AND t4.ClickHour=t.ClickHour AND t4.channel = t.channel)
--,ipApp = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.ip=t.ip AND t1.app = t.app)
--,ipAppOS = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.ip=t.ip AND t1.app = t.app AND t1.OS = t.OS)
--,ipDevice = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.ip=t.ip AND t1.device = t.device)
--,AppOS = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.app=t.app AND t1.OS = t.OS)
--,AppOSChannel = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.app=t.app AND t1.OS = t.OS AND t1.channel = t.channel)
--,ipChannel = (SELECT COUNT(*) FROM trainSubSet t1 WHERE t1.ip=t.ip AND t1.channel = t.channel)
INTO testFeaEng
FROM CleantestFraud t



SELECT TOP 1 * FROM trainFeaEng
SELECT TOP 1 * FROM testFeaEng

--ALTER TABLE dfFinal ALTER COLUMN is_attributed INT;
SELECT click_id,is_attributed FROM dfFinal;
