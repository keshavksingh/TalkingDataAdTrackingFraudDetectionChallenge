import pyodbc
connection=pyodbc.connect(r'DRIVER={SQL Server};SERVER=KESIN\DEV;DATABASE=dev;Trusted_Connection=yes')
sqlQuery="SELECT app,device,os,channel,ClickHour,ipHourAppCount,ipHourDeviceCount,ipHourOSCount,ipHourChannelCount,is_attributed FROM trainFeaEng"
#cursor=connection.cursor()
#cursor.execute("SELECT TOP 1 * FROM CleantrainFraud")
##tables=cursor.fetchall()
import pandas as pd
df=pd.io.sql.read_sql(sqlQuery,connection)
df.head()
import numpy as np
X = df[['app','device','os','channel','ClickHour','ipHourAppCount','ipHourDeviceCount','ipHourOSCount','ipHourChannelCount']]
y = df[['is_attributed']]
from sklearn.cross_validation import train_test_split
x_train, x_test, y_train, y_test = train_test_split(X, y, test_size = 0.25, random_state = 101)
y_test.is_attributed.unique()
list(x_train)
import lightgbm as lgb
y=y_train.values.ravel()
d_train = lgb.Dataset(x_train, label=y,feature_name=['app','device','os','channel','ClickHour','ipHourAppCount','ipHourDeviceCount','ipHourOSCount','ipHourChannelCount'],categorical_feature=['app','device','os','channel','ClickHour'])
y_Val=y_test.values.ravel()
d_EVAL = lgb.Dataset(x_test, label=y_Val,feature_name=['app','device','os','channel','ClickHour','ipHourAppCount','ipHourDeviceCount','ipHourOSCount','ipHourChannelCount'],categorical_feature=['app','device','os','channel','ClickHour'])

params = {}
params['learning_rate'] = 0.1 ##tried learning rate .01 seems to be too less
#params['categorical_feature'] = 0,1,2,3,4,5
params['boosting_type'] = 'gbdt'
params['objective'] = 'binary'
params['metric'] = 'binary_logloss'##'auc'
params['sub_feature'] = 0.5
params['num_leaves'] = 7
#params['min_data'] = 50
params['max_depth'] = 3
params['min_child_samples']=100

params['max_bin']=100
params['subsample']=0.7
params['subsample_freq']=1
params['colsample_bytree']=0.7
params['min_child_weight']=0
params['min_split_gain']=0

params['scale_pos_weight']=99.75
#clf = lgb.train(params, d_train, 200)
clf = lgb.train(params, d_train, num_boost_round=1000,valid_sets=d_EVAL,early_stopping_rounds=50)

#Prediction
y_pred=clf.predict(x_test)
y_pred
#convert into binary values
for i in range(0,len(y_pred)):
    if y_pred[i]>=.5:
        y_pred[i]=1
    else:
        y_pred[i]=0

#Confusion matrix
from sklearn.metrics import confusion_matrix
cm = confusion_matrix(y_test, y_pred)
print(cm)
#Accuracy
from sklearn.metrics import accuracy_score
accuracy=accuracy_score(y_pred,y_test)
print(accuracy)

connection=pyodbc.connect(r'DRIVER={SQL Server};SERVER=KESIN\DEV;DATABASE=dev;Trusted_Connection=yes')
sqlTestQuery="SELECT click_id,app,device,os,channel,ClickHour,ipHourAppCount,ipHourDeviceCount,ipHourOSCount,ipHourChannelCount FROM testFeaEng ORDER BY click_id asc"
dfTest=pd.io.sql.read_sql(sqlTestQuery,connection)

dfScore=dfTest[['app','device','os','channel','ClickHour','ipHourAppCount','ipHourDeviceCount','ipHourOSCount','ipHourChannelCount']]

#Prediction
yScore=clf.predict(dfScore)
#convert into binary values
for i in range(0,len(yScore)):
    if yScore[i]>=.5:
        yScore[i]=1
    else:
        yScore[i]=0
res=pd.DataFrame(yScore)
res.columns=['is_attributed']
dfFinal=pd.concat([dfTest['click_id'],res], axis=1)

res.is_attributed.unique()

dfFinal.to_csv("C:/Keshav/DataScience/TalkingDataAdTrackingFraudDetectionChallenge/dfFinal.csv")
dfFinal.head()