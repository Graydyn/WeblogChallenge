from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import r2_score
import pandas as pd
import re
from datetime import datetime
from scipy import sparse

#putting this code into its own file since it has all the machine learning specific imports that people may not have installed
#tries to predict the size of a returned request based off of the parameters of a request


def parseLog(line):
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

infile = open('data/2015_07_22_mktplace_shop_web_log_sample.log')
rows = []
for line in infile:
    row = parseLog(line)
    rows.append(row)

columns = ['timestamp', 'elb', 'client_port', 'backend_port','request_processing_time','backend_processing_time','response_processing_time','elb_status_code','backend_status_code','received_bytes','sent_bytes','request_type','request_url']

df = pd.DataFrame(rows, columns=columns)

#first clean up the urls, the parameters will blow our dimensions out so ditch them (and hope they aren't too important to the sent_bytes)
def trimUrl(x):
	splitX = x.split('?')[0].split('/')
	url = splitX[2]
	if len(splitX) >= 4:
		return splitX[3]
		url = url + splitX[3]
	if len(splitX) >= 5:
		url = url + splitX[4]
	return url
df['request_url'] = df['request_url'].apply(trimUrl)

y = df['sent_bytes']

#grab just the columns that we know before a response is made, and nothin unique such as ids or timestamps
X = df[['request_processing_time', 'backend_processing_time', 'elb_status_code', 'backend_status_code', 'received_bytes']]


print "starting one-hotting, this may take a few minutes"
#one-hot the categoricals
url_dummies = pd.get_dummies(df['request_url'])
request_dummies = pd.get_dummies(df['request_type'])
#url_dummies = sparse.csr_matrix(df['request_url'].values)
X = X.join(url_dummies)
X = X.join(request_dummies)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0)

#training a gradient booster, which is just like a random forest except better in every possible way
print "training"
booster = GradientBoostingRegressor() #just a toy, so not going to tune the hyperparameters today
booster.fit(X_train, y_train)
print "predicting"
predictions = booster.predict(X_test)
print r2_score(y_test, predictions)