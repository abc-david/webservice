'''
Created on 20 mai 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import feedparser
import urllib2
from tidylib import tidy_document
from bs4 import BeautifulSoup
import html2text
import markdown

url = "http://www.puretrend.com/rss/news_t0.xml"
#url = "http://feedparser.org/docs/examples/rss20.xml"
d = feedparser.parse(url)
#print d.bozo
#print d
#for entry in d['entries']:
#    content = urllib2.urlopen(entry['link']).read()
#    print content
content = urllib2.urlopen(d.entries[0]['link']).read()
document, errors = tidy_document(content)
not_yet_text = html2text.html2text(content)
html = markdown.markdown(not_yet_text)
print html
#soup = BeautifulSoup(document)
#print soup.prettify()

for item in d.feed:
    print item