## this script reads in URLS that appear to be for a specific product
## It then looks at all of the products that a user has viewed, and tries to predict what they will view next
## This is known as a "User Based Collaborative Filter"


import pandas as pd
import re
from datetime import datetime

from sklearn.neighbors import KNeighborsRegressor
from sklearn import grid_search
from sklearn.cross_validation import train_test_split
from sklearn.metrics import r2_score

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

#if the URL is viewing a specific product, find which one
def findProductView(url):
    if re.findall(r'/shop/\w/', url):  #products appear to all have the pattern /shop/<some letter>
        product = url.split('/')
        if len(product) >= 6:
            product = product[5]
            if ' ' in product:
                product = product.split(' ')[0]
            if (product.count('-') > 4) and not ('?' in product): #sometimes we grab non products, but they follow a pattern of having lots of -s
                product_parts = product.split('-')
                product = ""
                for part in product_parts:
                    if part.islower():
                        product = product + part + ' '  #trim off the codes at the end of each product
                return product
            else: return 'None'
        else: return 'None'
    else: return 'None'

df['product'] = df['request_url'].apply(findProductView)

df = df[df['product'] != 'None']

df = df[df.groupby('product')['product'].transform(len) > 4] #remove any products only viewed a few imtes, as we can't really make predictions on that many samples, and they are unlikely to be recommended anyways

print str(len(df)) + " product views"
print str(len(set(df['product']))) + " distinct products"

df = df[['client_port','product']]

dummies = pd.get_dummies(df['product'])
df = df.join(dummies)
df = df.drop('product', axis=1)

df = df.groupby('client_port').sum()
#df.drop('client_port', axis=1)

models = {}
print "training model - trained OneVsRest on very many classes, so you might want to grab a coffee"
for column in df.columns:
    y = df[column]
    X=df.drop(column, axis=1)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
    # not going to train hyperparameters in this toy example, but that would normally go here
    nn = KNeighborsRegressor(weights = 'distance', n_neighbors = 5)
    nn.fit(X_train, y_train)
    predictions = nn.predict(X_test)
    models[column] = nn
    #print "r2 score " + str(r2_score(y_test, predictions))

highestProb = 0
highestProbLabel = ""
singleUser = df[123]
for column in df.columns:
    model = models[column]
    X = singleUser.drop(column, axis=1)
    prediction = model.predict(X)
    if prediction[0] > highestProb:
        highestProb = prediction[0]
        highestProbLabel = column
print "for user that has viewed the products : "
for column in df.columns:
    if singleUser[column] == 1:
        print column

print "the most likely page next product to be viewed is : " + highestProbLabel
print "with a probability of " + str(highestProb)


print df.columns[predictions.index(predictions.max)]
