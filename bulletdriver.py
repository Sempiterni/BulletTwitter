# bullettestdriver

from bullettwitter import BulletTwitter
from collections import deque
import ujson as json
import os.path
import requests.packages.urllib3
import time
requests.packages.urllib3.disable_warnings()

BT = BulletTwitter()
BT.load_keys('keys - justins.tsv')

#-------------------------------------------------
# gather all tweets with specific keywords
# searchList = ['#swag']
# counter = 0
# fout = open('fred.json', 'w')
# for tweet in BT.search(searchList):
#     json.dump(tweet, fout)
#     fout.write('\n')
#     counter += 1
#     if counter % 100 == 0:
#         print 'Found %d tweets' % counter
# print 'Found %d tweets' % counter        

# ---------------------------------------
# Hydrate a list of tweet ids

filename = '500tweets.txt'

search_list = deque()
fin = open(filename, 'r')

search_list = deque()

for anid in fin:
    anid = anid.rstrip()
    search_list.append(anid)

print len(search_list), 'ids loaded...'

of_name = filename.split(".")[0] + '_hydrated.json'

fout = open(of_name, 'w')

#convert list to deque for pop

BT = BulletTwitter()
BT.load_keys('keys - justins.tsv')

# This function simply starts data gathering. If data is not consumed
# then a queue timeout will shutdown the program.
BT.hydrate_tweets(search_list)

# Consume and process the data. Since we do not want excess processing in the data gathering,
# all processing of the json string must be done now.
counter = 0
while BT.is_done() == False:
	next = BT.get_queue()
	for tweet in next:
		tweet = json.loads(str(tweet))
		json.dump(tweet, fout)
		fout.write('\n')
		counter += 1
		if counter % 1000 == 0:
		    print 'Found %d tweets' % counter
print 'Found %d tweets' % counter  
#----------------------------------------

# Gathering timelines
# file = 'samplehits5.txt'

# searchList = list()
# fin = open(file, 'r') 

# for anid in fin:
#     anid = anid.rstrip().split(' ')
#     anid = anid[0]
#     anid = int(anid)
#     searchList.append(anid)

# BT.get_timelines(searchList)
