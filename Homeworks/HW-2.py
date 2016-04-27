# -*- coding: utf-8 -*-

# Name: Srinivas Avireddy
# Email: saviredd@eng.ucsd.edu
# PID: A53101356
from pyspark import SparkContext
sc = SparkContext()


def print_count(rdd):
    print 'Number of elements:', rdd.count()


f = open("../Data/hw2-files-final.txt","r")
files = [line.strip() for line in f.readlines()]
RDD  = sc.textFile(",".join(files)).cache()
print_count(RDD)

import ujson

def safe_parse(raw_json):
    try:
        tweet = ujson.loads(raw_json)
        return (tweet['user']['id_str'],tweet['text'].encode('UTF-8'))
    except:
        return 0

users = RDD.map(lambda x: safe_parse(x))\
            .filter(lambda x: x!=0).cache()

group_RDD = users.map(lambda x: (x[0],1))\
                 .reduceByKey(lambda x,y: x+y).cache()

def print_users_count(count):
    print 'The number of unique users is:', count

print_users_count(group_RDD.count())

import cPickle as pickle
partition = pickle.load(open("../Data/users-partition.pickle","rb"))

def find_partition_id(user):
    if user not in partition:
        return 7
    else:
        return partition[user]


group_RDD = group_RDD.map(lambda x: (find_partition_id(x[0]),x[1]))\
                 .reduceByKey(lambda x,y: x+y)\
                 .sortByKey(True)
                
def print_post_count(counts):
    for group_id, count in counts:
        print 'Group %d posted %d tweets' % (group_id, count)

print_post_count(group_RDD.collect())

# %load happyfuntokenizing.py
#!/usr/bin/env python

"""
This code implements a basic, Twitter-aware tokenizer.

A tokenizer is a function that splits a string of text into words. In
Python terms, we map string and unicode objects into lists of unicode
objects.

There is not a single right way to do tokenizing. The best method
depends on the application.  This tokenizer is designed to be flexible
and this easy to adapt to new domains and tasks.  The basic logic is
this:

1. The tuple regex_strings defines a list of regular expression
   strings.

2. The regex_strings strings are put, in order, into a compiled
   regular expression object called word_re.

3. The tokenization is done by word_re.findall(s), where s is the
   user-supplied string, inside the tokenize() method of the class
   Tokenizer.

4. When instantiating Tokenizer objects, there is a single option:
   preserve_case.  By default, it is set to True. If it is set to
   False, then the tokenizer will downcase everything except for
   emoticons.

The __main__ method illustrates by tokenizing a few examples.

I've also included a Tokenizer method tokenize_random_tweet(). If the
twitter library is installed (http://code.google.com/p/python-twitter/)
and Twitter is cooperating, then it should tokenize a random
English-language tweet.


Julaiti Alafate:
  I modified the regex strings to extract URLs in tweets.
"""

__author__ = "Christopher Potts"
__copyright__ = "Copyright 2011, Christopher Potts"
__credits__ = []
__license__ = "Creative Commons Attribution-NonCommercial-ShareAlike 3.0 Unported License: http://creativecommons.org/licenses/by-nc-sa/3.0/"
__version__ = "1.0"
__maintainer__ = "Christopher Potts"
__email__ = "See the author's website"

######################################################################

import re
import htmlentitydefs

######################################################################
# The following strings are components in the regular expression
# that is used for tokenizing. It's important that phone_number
# appears first in the final regex (since it can contain whitespace).
# It also could matter that tags comes after emoticons, due to the
# possibility of having text like
#
#     <:| and some text >:)
#
# Most imporatantly, the final element should always be last, since it
# does a last ditch whitespace-based tokenization of whatever is left.

# This particular element is used in a couple ways, so we define it
# with a name:
emoticon_string = r"""
    (?:
      [<>]?
      [:;=8]                     # eyes
      [\-o\*\']?                 # optional nose
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth      
      |
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth
      [\-o\*\']?                 # optional nose
      [:;=8]                     # eyes
      [<>]?
    )"""

# The components of the tokenizer:
regex_strings = (
    # Phone numbers:
    r"""
    (?:
      (?:            # (international)
        \+?[01]
        [\-\s.]*
      )?            
      (?:            # (area code)
        [\(]?
        \d{3}
        [\-\s.\)]*
      )?    
      \d{3}          # exchange
      [\-\s.]*   
      \d{4}          # base
    )"""
    ,
    # URLs:
    r"""http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"""
    ,
    # Emoticons:
    emoticon_string
    ,    
    # HTML tags:
     r"""<[^>]+>"""
    ,
    # Twitter username:
    r"""(?:@[\w_]+)"""
    ,
    # Twitter hashtags:
    r"""(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"""
    ,
    # Remaining word types:
    r"""
    (?:[a-z][a-z'\-_]+[a-z])       # Words with apostrophes or dashes.
    |
    (?:[+\-]?\d+[,/.:-]\d+[+\-]?)  # Numbers, including fractions, decimals.
    |
    (?:[\w_]+)                     # Words without apostrophes or dashes.
    |
    (?:\.(?:\s*\.){1,})            # Ellipsis dots. 
    |
    (?:\S)                         # Everything else that isn't whitespace.
    """
    )

######################################################################
# This is the core tokenizing regex:
    
word_re = re.compile(r"""(%s)""" % "|".join(regex_strings), re.VERBOSE | re.I | re.UNICODE)

# The emoticon string gets its own regex so that we can preserve case for them as needed:
emoticon_re = re.compile(regex_strings[1], re.VERBOSE | re.I | re.UNICODE)

# These are for regularizing HTML entities to Unicode:
html_entity_digit_re = re.compile(r"&#\d+;")
html_entity_alpha_re = re.compile(r"&\w+;")
amp = "&amp;"

######################################################################

class Tokenizer:
    def __init__(self, preserve_case=False):
        self.preserve_case = preserve_case

    def tokenize(self, s):
        """
        Argument: s -- any string or unicode object
        Value: a tokenize list of strings; conatenating this list returns the original string if preserve_case=False
        """        
        # Try to ensure unicode:
        try:
            s = unicode(s)
        except UnicodeDecodeError:
            s = str(s).encode('string_escape')
            s = unicode(s)
        # Fix HTML character entitites:
        s = self.__html2unicode(s)
        # Tokenize:
        words = word_re.findall(s)
        # Possible alter the case, but avoid changing emoticons like :D into :d:
        if not self.preserve_case:            
            words = map((lambda x : x if emoticon_re.search(x) else x.lower()), words)
        return words

    def tokenize_random_tweet(self):
        """
        If the twitter library is installed and a twitter connection
        can be established, then tokenize a random tweet.
        """
        try:
            import twitter
        except ImportError:
            print "Apologies. The random tweet functionality requires the Python twitter library: http://code.google.com/p/python-twitter/"
        from random import shuffle
        api = twitter.Api()
        tweets = api.GetPublicTimeline()
        if tweets:
            for tweet in tweets:
                if tweet.user.lang == 'en':            
                    return self.tokenize(tweet.text)
        else:
            raise Exception("Apologies. I couldn't get Twitter to give me a public English-language tweet. Perhaps try again")

    def __html2unicode(self, s):
        """
        Internal metod that seeks to replace all the HTML entities in
        s with their corresponding unicode characters.
        """
        # First the digits:
        ents = set(html_entity_digit_re.findall(s))
        if len(ents) > 0:
            for ent in ents:
                entnum = ent[2:-1]
                try:
                    entnum = int(entnum)
                    s = s.replace(ent, unichr(entnum))  
                except:
                    pass
        # Now the alpha versions:
        ents = set(html_entity_alpha_re.findall(s))
        ents = filter((lambda x : x != amp), ents)
        for ent in ents:
            entname = ent[1:-1]
            try:            
                s = s.replace(ent, unichr(htmlentitydefs.name2codepoint[entname]))
            except:
                pass                    
            s = s.replace(amp, " and ")
        return s

from math import log

tok = Tokenizer(preserve_case=False)

def get_rel_popularity(c_k, c_all):
    return log(1.0 * c_k / c_all) / log(2)


def print_tokens(tokens, gid = None):
    group_name = "overall"
    if gid is not None:
        group_name = "group %d" % gid
    print '=' * 5 + ' ' + group_name + ' ' + '=' * 5
    for t, n in tokens:
        print "%s\t%.4f" % (t, n)
    print


def user_tokenize(tokens,user):
    token_list = []
    for token in tokens:
        token_list.append((token,user))
    return token_list

unique_user_tokens = users.map(lambda x:(tok.tokenize(x[1]),x[0]))\
              .flatMap(lambda x:user_tokenize(x[0],x[1]))\
              .distinct()
#print unique_user_tokens.reduceByKey(lambda x,y:x+y).take(10)
unique_user_tokens_grouped = unique_user_tokens.groupByKey().cache()
print_count(unique_user_tokens_grouped)

filtered_tokens = unique_user_tokens_grouped.filter(lambda x: len(x[1]) >= 100).cache()

'''
sorted_filtered_tokens_count = filtered_tokens.map(lambda x: (len(x[1]),x[0]))\
                                    .sortByKey(False)\
                                    .map(lambda (b,a):(a.encode('UTF-8'),b))
'''

sorted_filtered_tokens_count = filtered_tokens.map(lambda x: ((-len(x[1]),x[0]),x[0]))\
                                   .sortByKey(True)\
                                   .map(lambda x:(x[1].encode('UTF-8'),-x[0][0]))
print_count(sorted_filtered_tokens_count)
print_tokens(sorted_filtered_tokens_count.take(20))


def token_group(token):
    group_tuples = []
    length = len(token[1])
    for user in token[1]:
        group_tuples.append((token[0],find_partition_id(user),length))
    return group_tuples


group_tokens = filtered_tokens.flatMap(lambda x:token_group(x)).map(lambda x:((x[0],x[1],x[2]),1))\
                              .reduceByKey(lambda x,y: x+y)\
                                .map(lambda x:(((-get_rel_popularity(x[1],x[0][2]),x[0][0]),(x[0][0],x[0][1]))))\
                                .sortByKey(True)\
                                 .map(lambda x: ((x[1][0].encode('UTF-8'),-x[0][0],x[1][1])))
for i in xrange(0,8):
    reduce_rdd = group_tokens.filter(lambda x: x[2] == i).map(lambda x:(x[0],x[1]))
    #print reduce_rdd.take(5)
    print_tokens(reduce_rdd.take(10),i)
#group_tokens.take(5)


