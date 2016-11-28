#run through spark-submit, or the spark context will be missing

import pandas as pd
import numpy as np
import re
from datetime import datetime
from datetime import timedelta


#takes a line from weblogs and splits it into fields
#for convenience, this only grabs up until the request_url
#sample weblog line:
#2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
def custom_split(line):
    line_parts = re.findall(r"[\w\"\-\/\.:\?\=\&]+", line)

    timestamp = datetime.strptime(line_parts[0], '%Y-%m-%dT%H:%M:%S.%fZ')
    elb = line_parts[1]
    client_port = line_parts[2].split(':')[0] #using ip address as an identifier, so I feel that it makes sense to ditch the port number
    backend_port = line_parts[3]
    request_processing_time = float(line_parts[4])
    backend_processing_time = float(line_parts[5])
    response_processing_time = float(line_parts[6])
    elb_status_code = int(line_parts[7])
    backend_status_code = int(line_parts[8])
    received_bytes = int(line_parts[9])
    sent_bytes = int(line_parts[10])
    request_type = line_parts[11]
    request_url = line_parts[12]

    return [timestamp, elb, client_port, backend_port,request_processing_time,backend_processing_time,response_processing_time,elb_status_code,backend_status_code,received_bytes,sent_bytes,request_type,request_url]

#opens a local file and maps it to an RDD, to simulate working from a clustered environment
#It then formats the RDD as a DataFrame, and switches to the Pandas API for more convenient processing
def createDf():
    dataRdd = sc.textFile('data/2015_07_22_mktplace_shop_web_log_sample.log')
    splitRdd = dataRdd.map(custom_split)

    columns = ['timestamp','elb','client_port','backend_port','request_processing_time','backend_processing_time',
           'response_processing_time','elb_status_code','backend_status_code','received_bytes','sent_bytes',
           'request_type', 'request_url']

    df = splitRdd.toDF().toPandas()
    df.columns = columns
    return df

#create session ids by comparing each timestamp to the next timestamp is series and seeing if greater than our alloted time have passed
#session_timeout controls how long can have passed between requests before we consider it a new session
def sessionize(df, session_timeout):
    df_by_ip = df.groupby('client_port')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    df['session_id'] = df_by_ip['timestamp'].apply(lambda x: (x - x.shift(1) > pd.Timedelta(str(session_timeout) + " min")).cumsum())

    return df

#prints the average session duration
#Considers a session with one call to have zero duration, which may require alteration depending on how the information is used
def printAverageSessionDurations(df):
    #assuming minimum session time of 0 minutes
    session_times = df.groupby(['client_port','session_id'])['timestamp'].apply(lambda x: max(x) - min(x))
    average_time = np.mean(session_times)
    print "average session time if we call a single call a 0 minute session " + str(average_time)
    return average_time

#adds a column to our dataframe for how many unique urls were called during each session.  Also prints out an example of the result.
def countUniqueUrlsPerSession(df):
    df['session_urls'] = df.groupby(['client_port','session_id'])['request_url'].transform('nunique')

    print "our dataframe now contains a column that shows how many unique urls were hit in each session"
    print "example: user with ip address 123.242.248.130 on there first session viewed " + str(df[df['client_port'] == '123.242.248.130']['session_urls'][0]) + " urls" 
    print "average number of urls hit per session : " + str( np.mean(df['session_urls']))
    return df

#finds the user with the longest session time
#threshold is in seconds, if a threshold is supplied, it will filter out users past this point.  For the reasoning behind this please see the accompanying notebook.
def findMaxSessionTimes(df, threshold):
    session_times = df.groupby(['client_port','session_id'])['timestamp'].apply(lambda x: max(x) - min(x))
    session_times_seconds = session_times.apply(lambda x: x.seconds)
    human_session_times = session_times_seconds[session_times_seconds < threshold]
    print "ip address of user with longest session time that does not exceed " + str(threshold) + " seconds : "  + str(human_session_times.idxmax()[0])
    print "duration of max length session that doesn not exceed " + str(threshold) +" seconds : " + str(human_session_times.max() / 60.0) + " minutes"
    return human_session_times.idxmax()[0]

if __name__ == "__main__":
    df = createDf()
    df = sessionize(df, 15)
    printAverageSessionDurations(df)
    countUniqueUrlsPerSession(df)
    findMaxSessionTimes(df)
