# Python Twitter Crawler using bear python
# Author: Justin Sampson

import tarfile

# import sys

import twitter
import ujson as json
import re
import string
import operator
import codecs
import time
import os
import threading
import Queue
from collections import deque


class BulletTwitter:
    def __init__(self):
        self.keys = []
        self.timers = dict()

        self.logging = False
        self.numThreads = 0
        self.lock = threading.Lock()
        self.callLocks = list()
        self.tweetQueue = Queue.Queue(10000)
        self.doneSearching = False
        
    def load_keys(self, filename):
        with open(filename, 'r') as fKeysIn:
            nkeys = 0
            ntried = 0
            for line in fKeysIn:
                ntried += 1
                line = line.rstrip().split('\t')
                auth = twitter.Api(consumer_key = line[0], consumer_secret = line[1], access_token_key = line[2], access_token_secret = line[3])
                try:
                    auth.VerifyCredentials()
                    self.keys.append(auth)
                    nkeys += 1
                except:
                    pass
            print "%d / %d keys are live" % (nkeys, ntried)
        
        for i in range(0, len(self.keys)):
            self.timers[i] = [0, 0]
            self.callLocks.append(threading.Lock())
    
    # This function takes a list of keywords to search
    # Results are returned as a generated list of tweets
    def search(self, keywords):
        while len(keywords) > 0:
            if self.numThreads < len(self.keys):
                keyword = keywords.pop(0)
                aThread = threading.Thread(target=self.start_search, args=(keyword,))
                aThread.daemon = True
                aThread.start()
                self.numThreads += 1
            else:
                print 'Waiting on available thread - alive: %d, limited: %d' % (self.numThreads, self.get_num_rate_limited())
                print 'Next thread available in %d seconds...' % self.get_next_avail_time()
                # return whatever is currently in the queue to the user
                queueLen = self.tweetQueue.qsize()
                for item in xrange(queueLen):
                    yield self.tweetQueue.get()
                time.sleep(1)
        while self.numThreads > 0 or self.tweetQueue.qsize() > 0:
            print 'Waiting on threads to end - # threads running: %d' % self.numThreads
            # return whatever is currently in the queue to the user
            queueLen = self.tweetQueue.qsize()
            for item in xrange(queueLen):
                yield self.tweetQueue.get()
            time.sleep(1)
    
    # Worker function for a search thread
    def start_search(self, keyword):
        tweets = None

        doneWithUser = False
        lastID = 0
        totCount = 0
        while not doneWithUser:
            rateLimited = False
            result = self.find_available_stream()
            self.callLocks[result].acquire()
            try:
                if (lastID == 0):
                    # print 'Attempting ' + str(keyword) + ' with account ' + str(result)
                    tweets = self.keys[result].GetSearch(term=keyword)
                else:
                    # print 'Attempting ' + str(keyword) + ' with account ' + str(result) + ' next 15'
                    tweets = self.keys[result].GetSearch(term=keyword, max_id = lastID)
            except twitter.TwitterError as e:
                print e
                print 'Exception while attempting ' + str(keyword) + ' with account ' + str(result)
                try:
                    if e[0][0]['code'] == 88:
                        self.extend_timer(result, '/search/tweets') 
                        rateLimited = True
                except Exception as e:
                    print e
                    pass
            self.callLocks[result].release()
            # This makes sure the user isn't skipped simply because a crawler
            # got rate limited.  
            if rateLimited == False:
                if tweets != None:
                    for tweet in tweets:
                        tweet = json.loads(str(tweet))
                        self.tweetQueue.put(tweet)
                        lastID = tweet['id']
                    totCount += len(tweets)
                    # Stopping criteria - zip the file and add it to archive
                    if totCount >= 100000 or len(tweets) < 15:
                        doneWithUser = True
                # else:
                    # doneWithUser = True
        self.numThreads -= 1
    
    # Timing code to find a stream that is not currently rate limited
    def find_available_stream(self):
        self.lock.acquire()
        result = -1
        try:
            # look for a stream that is not limited
            while result == -1:
                for i in range(0, len(self.keys)):
                    if self.timers[i][1] == 180 and self.timers[i][0] + 900 < time.time():
                        self.timers[i][0] = 0
                        self.timers[i][1] = 0
                    if self.timers[i][0] == 0 and self.callLocks[i].locked() == False:
                        result = i
                        break
                    elif self.timers[i][1] < 180 and self.callLocks[i].locked() == False: 
                        result = i
                        break
                if result == -1: # case when all streams are rate limited
                    time.sleep(1)
                    
            # check if profile exists and write if so
            # update self.timers so that we know our 15 minute window has begun
            # if the window has already begun update the request number
            if self.timers[result][0] == 0:
                self.timers[result][0] = time.time()
                self.timers[result][1] = 1
            elif self.timers[result][1] < 180:
                self.timers[result][1] += 1
        finally:
            self.lock.release()  
        return result
        
    # Timing function used to extend the timer if a stream gets unexpectedly limited
    def extend_timer(self, result, resource):
        self.lock.acquire()
        try:
            # this is kind of silly but it works
            # if account gets rate limited make sure its not pulled again
            # and adjust the time window so it will recheck in half the rate limit window
            try:
                waitTime = self.keys[result].GetSleepTime(resource)
            except Exception as e:
                print e
            self.timers[result][1] = 180
            self.timers[result][0] = time.time() - (900 - waitTime)
        finally:
            self.lock.release()
            
    # This function returns the minimum number of seconds until a stream will become available
    # results returned in seconds
    def get_next_avail_time(self):
        min_time = 9999999999999
        # print self.timers
        for key, val in self.timers.iteritems():
            if val[0] < min_time:
                min_time = val[0]
        return 900 + min_time - time.time() 
    
    # This function returns the number of streams currently rate limited
    def get_num_rate_limited(self):
        count = 0
        for key, val in self.timers.iteritems():
            if val[1] == 180:
                count += 1
        return count

    def hydrate_tweets(self, searchList):
        aThread = threading.Thread(target=self.hydrate_tweets_worker, args=(searchList,))
        aThread.daemon = True
        aThread.start()

        aThread = threading.Thread(target=self.get_status, args=(3,))
        aThread.daemon = True
        aThread.start()
        return

    # This function takes a list of tweet ids to hydrate
    # Results are returned as a generated list of tweets
    def hydrate_tweets_worker(self, searchList):
        while len(searchList) > 0:
            if self.numThreads < len(self.keys):
                # print self.numThreads, len(self.keys)

                tweetIDs = []
                # if the list has at least 100 items remaining pop them into a list
                while len(tweetIDs) < 100:
                    try:
                        tweetIDs.append(searchList.popleft())
                    except:
                        break

                # tweetID = searchList.popleft()
                # print tweetID
                aThread = threading.Thread(target=self.start_hydrate, args=(tweetIDs,))
                aThread.daemon = True
                aThread.start()
                self.numThreads += 1
        self.doneSearching = True
    
    def is_done(self):
        if self.doneSearching != True or self.get_queue_len() > 0 or self.numThreads > 0:
            return False
        else:
            return True

    def get_queue(self):
        queueLen = self.tweetQueue.qsize()
        chunk = list()
        for item in xrange(queueLen):
            chunk.append(self.tweetQueue.get())
        return chunk

    def get_queue_len(self):
        return self.tweetQueue.qsize()

    def get_status(self, delay = 3):
        while True:
            print 'Waiting on available thread - alive: %d, limited: %d' % (self.numThreads, self.get_num_rate_limited())
            print 'Next thread available in %d seconds...' % self.get_next_avail_time()
            time.sleep(delay)

    # Worker function for a tweet hydration thread
    def start_hydrate(self, anid):
        doneWithUser = False
        totCount = 0
        tweet = None
        while not doneWithUser:
            rateLimited = False
            result = -1
            deleted = False
            # look for a stream that is not limited
            result = self.find_available_stream()
            self.callLocks[result].acquire()
            try:
                try:
                    # print 'Attempting ' + str(anid) + ' with account ' + str(result)
                    tweets = self.StatusLookup(api=self.keys[result], ids=anid)
                except Exception as e:
                    # print e
                    deleted = True
                    try:
                        if e[0][0]['code'] == 88:
                            rateLimited = True
                            self.extend_timer(result, '/statuses/lookup')
                        if e[0][0]['code'] == 130: # over capacity
                            rateLimited = True
                        if e[0][0]['code'] == 144: # doesnt exist
                            deleted = True
                        if e[0][0]['code'] == 179: # access denied
                            deleted = True
                        if e[0][0]['code'] == 34: # page does not exist (deleted)
                            deleted = True
                        if e[0][0]['code'] == 63: # user suspended
                            deleted = True
                    except:
                        pass
                # This makes sure the user isn't skipped simply because a crawler
                # got rate limited.  
                if rateLimited == False:
                    if deleted != True:
                        for tweet in tweets:
                            self.tweetQueue.put(tweet)
                    doneWithUser = True
            finally:
                self.callLocks[result].release()
        self.numThreads -= 1
    
    def get_timelines(self, searchList):
        while len(searchList) > 0:
            if self.numThreads < len(self.keys):
                userID = searchList.pop(0)
                aThread = threading.Thread(target=self.start_get_timeline, args=(userID,))
                aThread.daemon = True
                aThread.start()
                self.numThreads += 1
            else:
                print 'Waiting on available thread - alive: %d, limited: %d' % (self.numThreads, self.get_num_rate_limited())
                print 'Next thread available in %d seconds...' % self.get_next_avail_time()
                time.sleep(1)
        while self.numThreads > 0:
            print 'Waiting on threads to end - # threads running: %d' % self.numThreads
            time.sleep(1)

    # worker function to hydrate user tweets    
    def start_get_timeline(self, userid):
        doneWithUser = False
        lastID = 0
        totCount = 0
        userTimeline = []

        fout = open(str(userid) + '.json', 'w')
        tweets = None
            
        while not doneWithUser:
            rateLimited = False
            result = -1
            # look for a stream that is not limited
            result = self.find_available_stream()
            self.callLocks[result].acquire()
            try:
                try:
                    if (lastID == 0):
                        tweets = self.keys[result].GetUserTimeline(screen_name = userid, count = 200)
                    else:
                        tweets = self.keys[result].GetUserTimeline(screen_name = userid, max_id = lastID, count = 200)
                except twitter.TwitterError as e:
                    # print e
                    try:
                        if type(e) is not list:
                            # we reach this when the user is private.
                            doneWithUser = True

                        if type(e) is list and e[0]['code'] == 88:
                            self.extent_timer(result, '/statuses/user_timeline') 
                            rateLimited = True
                    except Exception as e:
                        pass
                # This makes sure the user isn't skipped simply because a crawler
                # got rate limited.  
                if rateLimited == False:
                    if tweets == None:
                        pass
                    else:
                        for tweet in tweets:
                            # print tweet
                            tweet = json.loads(str(tweet))
                            json.dump(tweet, fout)
                            fout.write('\n')
                            lastID = tweet['id']
                        totCount += len(tweets)
                    # Stopping criteria - zip the file and add it to archive
                        if totCount >= 3200 or len(tweets) < 200:
                            doneWithUser = True
                else:
                    doneWithUser = True  
            finally:
                self.callLocks[result].release()  
        self.numThreads -= 1

    # Rewritten StatusLookup function that operates on 100 ids at a time
    def StatusLookup(self,
                    api=None,
                    ids=None,
                    trim_user=None,
                    map=None,
                    include_entities=True):

        from twitter import Status

        if api == None:
            print "A link to python-twitter API is required."
            return []

        if not ids:
            print "Specify at least one tweet ID."

        url = '%s/statuses/lookup.json' % api.base_url
        parameters = {}
        tids = list()
        if ids:
            tids.extend(ids)

        if len(tids):
            parameters['id'] = ','.join(["%s" % t for t in tids])
        if trim_user:
            parameters['trim_user'] = 'true'
        if not include_entities:
            parameters['include_entities'] = 'false'

        resp = api._RequestUrl(url, 'GET', data=parameters)

        try:
            data = api._ParseAndCheckTwitter(resp.content.decode('utf-8'))
        except TwitterError as e:
            _, e, _ = sys.exc_info()
            t = e.args[0]
            if len(t) == 1 and ('code' in t[0]) and (t[0]['code'] == 34):
                data = []
            else:
                raise
        try:
            results = [Status.NewFromJsonDict(u) for u in data]
        except Exception as e:
            print e

        return results