'''
Created on 6 june 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import codecs
from tidylib import tidy_document
from bs4 import BeautifulSoup
import html2text
import urllib2
from readability.readability import Document
import requests
import logging
import string
import collections
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)
#import httplib2
#import simplejson as json
import numpy
import HTMLParser
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
pars = HTMLParser.HTMLParser()
#import AlchemyAPI
#API_KEY = "83c788fc7c0db8a77f916f31aaa6887f3b735cf2"
#alchemyObj = AlchemyAPI.AlchemyAPI()
#alchemyObj.loadAPIKey(API_KEY)

INDENT = 3
SPACE = " "
NEWLINE = "\n"
SITE_URL = {"showroom" : "http://showroom-styliste.mystagingwebsite.com", \
            "special" : "http://specialmode.fr", \
            "mag" : "http://we-mag.info"}
log_file = "/home/david/logs/test_query_post_json.xml"

"""
with open('/media/freebox/Template Emailing/Tempo/mail_dedie_pcp.html') as f:
    soup = BeautifulSoup(f.read())
with open('/media/freebox/Template Emailing/Ohmymag/pretty_mail_dedie_pcp.html', 'w') as f:
    f.write(soup.prettify())
"""

class GetOutOfLoop( Exception ):
    pass

class Image():
    def __init__(self, json_object):
        self.json_object = json_object
        self.url = self.json_object['url']
        self.width = self.json_object['width']
        self.height = self.json_object['height']
        self.px_dim = [self.width, self.height]

class Images():
    def __init__(self, json_object):
        image_name_list = ['thumbnail', 'medium', 'large', 'full', 'slider', 'big-slider', \
                           'tie-large', 'tie-small', 'tie-medium', 'blog-x-large-size', 'blog-large-size', \
                           'featured-article-medium-size', 'featured-article-large-size', \
                           'article-tab-image-size', 'article-list-image-size', \
                           'gallery-thumb-size', 'gallery-image-size', \
                           'wysija-newsletters-max']
        self.json_object = json_object
        # Image
        if self.json_object:
            for image_name in image_name_list:
                if image_name in self.json_object.keys():
                    setattr(self, image_name, Image(self.json_object[image_name]))
    def __iter__(self):
        for attr, value in self.__dict__.iteritems():
            yield attr, value

class Attachment():
    def __init__(self, json_object):
        self.json_object = json_object
        self.description = self.json_object['description']
        self.parent = self.json_object['parent']
        self.id = self.json_object['id']
        self.mime_type = self.json_object['mime_type']
        self.title = self.json_object['title']
        self.url = self.json_object['url']
        self.slug = self.json_object['slug']
        self.caption = self.json_object['caption']
        # Images
        if 'images' in self.json_object.keys():
            self.images = Images(self.json_object['images'])

class Custom_fields():
    def __init__(self, json_object):
        self.json_object = json_object
        if 'wpe_sourcepermalink' in self.json_object.keys():
            self.wpe_source = self.json_object['wpe_sourcepermalink'][0]
        if 'wpe_feed' in self.json_object.keys():
            self.wpe_feed = self.json_object['wpe_feed'][0]
        if 'wpe_campaignid' in self.json_object.keys():
            self.wpe_campaign_id = self.json_object['wpe_campaignid'][0]

class Author():
    def __init__(self, json_object):
        self.json_object = json_object
        self.name = self.json_object['name']
        self.id = self.json_object['id']

class Tag():
    def __init__(self, json_object):
        self.json_object = json_object
        self.description = self.json_object['description']
        self.title = self.json_object['title']
        self.id = self.json_object['id']
        self.slug = self.json_object['slug']
        self.post_count = self.json_object['post_count']
    def __repr__(self):
        show_tag = [str(self.id), str(self.title), str(self.post_count)]
        return " | ".join(show_tag)

class Category():
    def __init__(self, json_object):
        self.json_object = json_object
        self.description = self.json_object['description']
        self.parent = self.json_object['parent']
        self.title = self.json_object['title']
        self.id = self.json_object['id']
        self.slug = self.json_object['slug']
        self.post_count = self.json_object['post_count']
    def __repr__(self):
        show_category = [str(self.id), str(self.parent), str(self.title), \
                         str(self.post_count)]
        return " | ".join(show_category)
    def append_root_category(self, root_category):
        self.root = root_category

class Post_args():
    def __init__(self, json_object):
        self.json_object = json_object
        self.status = self.json_object['status']
        self.title = self.json_object['title']
        self.url = self.json_object['url']
        self.tags = self.json_object['tags'] # returns a list
        self.modified = self.json_object['modified']
        self.content = self.json_object['content']
        self.date = self.json_object['date']
        self.type = self.json_object['type']
        self.id = self.json_object['id']
        self.comment_status = self.json_object['comment_status']
        self.slug = self.json_object['slug']
        # Text
        self.clean_title = pars.unescape(self.title)
        self.resume = self.json_object['excerpt']
        self.clean_resume = pars.unescape(self.resume)
        # Thumbnail
        if 'thumbnail' in self.json_object:
            self.thumbnail = self.json_object['thumbnail']
        else:
            self.thumbnail = False
            self.thumbnail_count = 0
        # TODO : Fix this.
        if 'thumbnail_images' in self.json_object.keys():
            if self.json_object['thumbnail_images']:
                self.thumbnail_images = Images(self.json_object['thumbnail_images'])
                #for image_x in list(self.thumbnail_images):
                #    print image_x.image_name
                self.thumbnail_count = len(self.thumbnail_images.json_object.keys())
                if 'thumnbnail' in self.thumbnail_images.json_object.keys():
                    self.thumbnail_url = self.thumbnail_images.thumbnail.url
                if 'full' in self.thumbnail_images.json_object.keys():
                    self.thumbnail_url_full = self.thumbnail_images.full.url
                if 'tie-large' in self.thumbnail_images.json_object.keys():
                    self.thumbnail_url_mail = self.thumbnail_images.tie_large.url
            else:
                self.thumbnail = False
                self.thumbnail_count = 0
        else:
            self.thumbnail = False
            self.thumbnail_count = 0

        # Custom_fields
        if self.json_object['custom_fields']:
            self.custom_fields = Custom_fields(self.json_object['custom_fields'])
            try:
                self.wpe_source = self.custom_fields.wpe_source
                self.wpe_feed = self.custom_fields.wpe_feed
                self.wpe_campaign_id = self.custom_fields.wpe_campaign_id
            except:
                self.wpe_feed = False
        else:
            self.wpe_feed = False
        # Author
        self.author = Author(self.json_object['author'])
        # Images (ie. Attachments)
        self.attachment_raw_data = self.json_object['attachments']
        if self.attachment_raw_data:
            if isinstance(self.attachment_raw_data, list):
                attachment_list = []
                image_list = []
                for attachment in self.attachment_raw_data:
                    this_attachment = Attachment(attachment)
                    attachment_list.append(this_attachment)
                    try:
                        image_list.append(this_attachment.images)
                    except:
                        pass
                self.attachments = attachment_list
                self.images = image_list
            else:
                self.attachments = self.attachment_raw_data
                self.images = self.attachments.images
            self.image_count = len(self.images)
        else:
            self.images = False
            self.image_count = 0
        # Categories
        self.categories = self.json_object['categories'] # returns a list
        self.category = Category(self.categories[0]) # TODO : what if multiple categories ?
        self.cat_id = self.category.id
        self.cat_title = self.category.title
        # Tags
        if 'tags' in self.json_object.keys():
            self.tags = self.json_object['tags'] # returns a list
            #print "tags:", self.tags
            if isinstance(self.tags, list):
                if len(self.tags) > 0:
                    self.first_tag = Tag(self.tags[0])
                    self.tag_list_obj = [Tag(self.tags[x]) for x in range(len(self.tags))]
                    self.tag_list = [str(tag.title) for tag in self.tag_list_obj]

    def __repr__(self):
        if self.wpe_feed:
            show_post = [str(self.id), str(self.cat_title), str(self.image_count), \
                         str(self.thumbnail_count), str(self.wpe_feed), str(self.title), str(self.thumbnail)]
        else:
            show_post = [str(self.id), str(self.cat_title), str(self.image_count), \
                         str(self.thumbnail_count), str(self.title), str(self.thumbnail)]
        return " | ".join(show_post)

    def print_raw_json(self, log_file):
        with open(log_file, 'w') as f:
            f.write(pprint_json(self.json_object))

    def template_eligible(self):
        if self.title and self.url and self.status == "publish":
            if self.thumbnail or self.images:
                #and self.resume
                return True
        return False

    def append_root_category(self, root_category):
        self.root_category = root_category

    def template_dict(self, short_text_char_max = 200):
        post_dict = {}
        post_dict['title'] = pars.unescape(self.clean_title).decode('utf-8')
        post_dict['title'] = self.title
        post_dict['href_url'] = pars.unescape(self.url).decode('utf-8')
        post_dict['category'] = pars.unescape(self.category.title).decode('utf-8')
        #post_dict['resume'] = pars.unescape(self.clean_resume).decode('utf-8')
        post_dict['short_text'] = pars.unescape(self.short_text(short_text_char_max)).decode('utf-8')
        try:
            post_dict['img_src'] = pars.unescape(self.thumbnail_url_mail).decode('utf-8')
            post_dict['img_dim'] = pars.unescape(self.thumbnail_images.tie_large.px_dim).decode('utf-8')
        except:
            post_dict['img_src'] = ""
            if self.thumbnail:
                post_dict['img_src'] = pars.unescape(self.thumbnail).decode('utf-8')
            #post_dict['image_dim'] = str(self.thumbnail_images.thumbnail.px_dim)
            elif self.images:
                if len(self.images) > 0:
                    #print self.images
                    try:
                        for item in self.images:
                            if item.json_object:
                                item_dict = dict(item)
                                item_dict.pop('json_object', None)
                                for image in item_dict.itervalues():
                                    if isinstance(image, Image):
                                        if image.url:
                                            post_dict['img_src'] = pars.unescape(image.url).decode('utf-8')
                                            raise GetOutOfLoop
                    except GetOutOfLoop:
                        pass
        post_dict['img_alt'] = pars.unescape(self.title).decode('utf-8')
        post_dict['call_to_action'] = pars.unescape("Lire la suite...").decode('utf-8')
        return post_dict

    def html(self):
        clean_html, ___ = tidy_document(self.content)
        return clean_html
    def pretty_html(self):
        soup = BeautifulSoup(self.html())
        return soup.prettify()
    def text(self):
        not_yet_text = html2text.html2text(self.html())
        return not_yet_text.replace("&nbsp_place_holder;", " ") \
            .replace("[**", "").replace("**]", " ").replace("**", "")
    def short_text(self, nb_char):
        words = string.split(self.text().replace(self.clean_title, ''))
        test_item = ["[", "]", "http", ".jpg", "html", "/", "_", "&amp"]
        selection = []
        length = []
        for word in words:
            skip = False
            for item in test_item:
                if item in word:
                    skip = True
            if len(word) > 40:
                skip = True
            if skip == False:
                selection.append(word)
                length.append(len(word))
        total = 0
        cpt_word = 0
        if len(selection) > 0:
            while total <= (nb_char - 3) and cpt_word < len(selection):
                total = total + length[cpt_word] + 1
                cpt_word += 1
            result = " ".join(selection[:(cpt_word - 1)])
            return pars.unescape(result + "...")
        else:
            return False
    def links(self):
        soup = BeautifulSoup(self.html())
        # TODO: should return the links not the a's
        return soup.find_all("a",href=True)
    def img_links(self):
        result = []
        soup = BeautifulSoup(self.html())
        img_tags = soup.find_all("img",src=True)
        #print "img_tag", img_tags
        for img_tag in img_tags:
            r = requests.get(img_tag['src'])
            if r.status_code == requests.codes.ok:
                #print "   status code OK"
                if "image" in r.headers['Content-Type']:
                    #print "    image OK"
                    image_spec = collections.OrderedDict()
                    attribute_list = ["src", "alt", "height", "width"]
                    for key in img_tag.attrs.keys():
                        if key in attribute_list:
                            if img_tag[key]:
                                image_spec[key] = img_tag[key]
                    #image_spec['post.id'] = self.id
                    result.append(image_spec)
        if result:
            return result
        else:
            return False
    def source_html(self):
        usock = urllib2.urlopen(self.wpe_source)
        data = usock.read()
        usock.close()
        clean_html, error_html = tidy_document(data)
        return clean_html
    def source_summary_html(self):
        #print "encoding =", Document(self.source_html()).encoding
        return Document(self.source_html()).summary()
    def source_pretty_html(self):
        soup = BeautifulSoup(self.source_summary_html())
        return soup.prettify()
    def source_text(self):
        not_yet_text = html2text.html2text(self.source_summary_html())
        not_yet_text = not_yet_text.replace("&nbsp_place_holder;", " ") \
            .replace("[**", "").replace("**]", " ").replace("**", "")
        #return not_yet_text.decode('utf-8').encode('latin-1')
        #return not_yet_text.encode('utf-8') #.encode('latin-1')
        #return not_yet_text.decode('utf-8').encode('latin1')
        try:
            return not_yet_text.decode('utf-8').encode('latin-1')
        except:
            try:
                return not_yet_text.decode('base64').encode('latin-1')
            except:
                return not_yet_text

class Post():
    def __init__(self, site=None, id=None, json=None):
        if json:
            self.json_object = json
        else:
            self.site = site
            self.id = id
            try:
                self.json_object = query_wp_json_post(site, id)
            except:
                print "Pb. with querying JSON API of the given WP site. Check site's URL and/or post's ID."
        #print pprint_json(self.json_object)
        self.json_status = self.json_object['status']
        self.previous_url = self.json_object['previous_url']
        self.next_url = self.json_object['next_url']
        self.post_args = Post_args(self.json_object['post'])
        self.status = self.post_args.status
        self.custom_fields = self.post_args.custom_fields
        self.wpe_source = self.post_args.wpe_source
        self.wpe_feed = self.post_args.wpe_feed
        self.wpe_campaign_id = self.post_args.wpe_campaign_id
        self.author = self.post_args.author
        self.title = self.post_args.title
        self.url = self.post_args.url
        self.tags = self.post_args.tags
        self.excerpt = self.post_args.excerpt
        self.modified = self.post_args.modified
        self.content = self.post_args.content
        self.date = self.post_args.date
        self.type = self.post_args.type
        self.check_id = self.post_args.id
        if not id:
            self.id = self.check_id
        self.categories = self.post_args.categories


    def html(self):
        clean_html, error_html = tidy_document(self.content)
        return clean_html
    def pretty_html(self):
        soup = BeautifulSoup(self.html())
        return soup.prettify()
    def text(self):
        not_yet_text = html2text.html2text(self.html())
        return not_yet_text.replace("&nbsp_place_holder;", " ") \
            .replace("[**", "").replace("**]", " ").replace("**", "")
    def short_text(self, nb_char):
        words = string.split(self.text())
        test_item = ["[", "]", "http", ".jpg", "html", "/", "_", "&amp"]
        selection = []
        length = []
        for word in words:
            skip = False
            for item in test_item:
                if item in word:
                    skip = True
            if len(word) > 40:
                skip = True
            if skip == False:
                selection.append(word)
                length.append(len(word))
        total = 0
        cpt_word = 0
        if len(selection) > 0:
            while total <= (nb_char - 3) and cpt_word < len(selection):
                total = total + length[cpt_word] + 1
                cpt_word += 1
            result = " ".join(selection[:(cpt_word - 1)])
            return result + "..."
        else:
            return False
    def links(self):
        soup = BeautifulSoup(self.html())
        # TODO: should return the links not the a's
        return soup.find_all("a",href=True)
    def img_links(self):
        result = []
        soup = BeautifulSoup(self.html())
        img_tags = soup.find_all("img",src=True)
        #print "img_tag", img_tags
        for img_tag in img_tags:
            r = requests.get(img_tag['src'])
            if r.status_code == requests.codes.ok:
                #print "   status code OK"
                if "image" in r.headers['Content-Type']:
                    #print "    image OK"
                    image_spec = collections.OrderedDict()
                    attribute_list = ["src", "alt", "height", "width"]
                    for key in img_tag.attrs.keys():
                        if key in attribute_list:
                            if img_tag[key]:
                                image_spec[key] = img_tag[key]
                    #image_spec['post.id'] = self.id
                    result.append(image_spec)
        if result:
            return result
        else:
            return False
    def source_html(self):
        usock = urllib2.urlopen(self.wpe_source)
        data = usock.read()
        usock.close()
        clean_html, error_html = tidy_document(data)
        return clean_html
    def source_summary_html(self):
        #print "encoding =", Document(self.source_html()).encoding
        return Document(self.source_html()).summary()
    def source_pretty_html(self):
        soup = BeautifulSoup(self.source_summary_html())
        return soup.prettify()
    def source_text(self):
        not_yet_text = html2text.html2text(self.source_summary_html())
        not_yet_text = not_yet_text.replace("&nbsp_place_holder;", " ") \
            .replace("[**", "").replace("**]", " ").replace("**", "")
        #return not_yet_text.decode('utf-8').encode('latin-1')
        #return not_yet_text.encode('utf-8') #.encode('latin-1')
        #return not_yet_text.decode('utf-8').encode('latin1')
        try:
            return not_yet_text.decode('utf-8').encode('latin-1')
        except:
            try:
                return not_yet_text.decode('base64').encode('latin-1')
            except:
                return not_yet_text

def htmlescape(text):
    text = (text).decode('utf-8')

    from htmlentitydefs import codepoint2name
    d = dict((unichr(code), u'&%s;' % name) for code,name in codepoint2name.iteritems() if code!=38) # exclude "&"
    if u"&" in text:
        text = text.replace(u"&", u"&amp;")
    for key, value in d.iteritems():
        if key in text:
            text = text.replace(key, value)
    return text

#print htmlescape("da&vid")

def pprint_json(o, level=0):
    ret = ""
    if isinstance(o, dict):
        ret += "{" + NEWLINE
        comma = ""
        for k,v in o.iteritems():
            ret += comma
            comma = ",\n"
            ret += SPACE * INDENT * (level+1)
            ret += '"' + str(k) + '":' + SPACE
            ret += pprint_json(v, level + 1)

        ret += NEWLINE + SPACE * INDENT * level + "}"
    elif isinstance(o, basestring):
        ret += '"' + o + '"'
    elif isinstance(o, list):
        ret += "[" + ",".join([pprint_json(e, level+1) for e in o]) + "]"
    elif isinstance(o, bool):
        ret += "true" if o else "false"
    elif isinstance(o, int):
        ret += str(o)
    elif isinstance(o, float):
        ret += '%.7g' % o
    elif isinstance(o, numpy.ndarray) and numpy.issubdtype(o.dtype, numpy.integer):
        ret += "[" + ','.join(map(str, o.flatten().tolist())) + "]"
    elif isinstance(o, numpy.ndarray) and numpy.issubdtype(o.dtype, numpy.inexact):
        ret += "[" + ','.join(map(lambda x: '%.7g' % x, o.flatten().tolist())) + "]"
    else:
        try:
            raise TypeError("Unknown type '%s' for json serialization" % str(type(o)))
        except:
            pass
    return ret.encode(encoding = 'utf-8')

def query_wp_json_categories(site):
    api_category = "?json=get_category_index"
    api_call = SITE_URL[site] + api_category
    r = requests.get(api_call)
    #print r
    #print r.json()
    return r.json()

#query_wp_json_categories('mag')

def query_categories(site, parent_id = 0):
# TODO : take care of subcategories
    json_categories = query_wp_json_categories(site = site)
    category_list = []
    for category in json_categories['categories']:
        if category['parent'] == parent_id and category['post_count'] > 0:
            this_category = Category(category)
            category_list.append(this_category)
    return category_list

def category_tree(site, parent_id = 0):
    json_categories = query_wp_json_categories(site = site)
    category_list = []
    category_by_id = {}
    for category in json_categories['categories']:
        this_category = Category(category)
        category_list.append(this_category)
        category_by_id[this_category.id] = this_category
    for category in category_list:
        if category.parent != parent_id:
            id_up = category.parent
            id_parent_list = [id_up]
            category_up = category_by_id[id_up]
            while id_up != 0:
                id_up = category_up.parent
                id_parent_list.append(id_up)
                if id_up != 0:
                    category_up = category_by_id[id_up]
            category.append_root_category(category_by_id[id_parent_list[-2]])
    return category_list, category_by_id

#cat_by_id = category_tree('special')[1]
#for cat_id, cat_des in cat_by_id.iteritems():
#    print cat_des

def query_wp_json_post(site, post_id):
    api_post_id = "/api/get_post/?post_id="
    api_call = SITE_URL[site] + api_post_id + str(post_id)
    r = requests.get(api_call)
    #print r
    #print r.json()
    return r.json()

#a=query_wp_json_post(site, 667)
#print pprint_json(a)

def query_wp_json_recent_posts(site, nb_post = 10):
    api_recent_post = "/api/get_recent_posts/"
    api_arg = ""
    if nb_post:
        api_arg = "?count=" + str(nb_post)

    api_call = SITE_URL[site] + api_recent_post + api_arg
    r = requests.get(api_call)
    #print "r", r
    #print "r.json()", pprint_json(r.json())
    return r.json()

def query_recent_posts(site, nb_post = 10, show_json = False):
    json_recent_posts = query_wp_json_recent_posts(site, nb_post = nb_post)
    post_list = []
    for json_post_candidate in json_recent_posts['posts']:
        if show_json:
            print pprint_json(json_post_candidate)
        this_post = Post_args(json_post_candidate)
        this_post.print_raw_json(log_file)
        post_list.append(this_post)
    return post_list

#post_list = query_recent_posts('special', 30)
#for post in post_list:
#    print post.cat_title, post.short_text(200)

def template_package(site, lookup_posts = 100, max_post_per_cat = 30, short_text_max_char = 200):
    template_dict = {}
    template_resume = {}
    category_list, category_by_id = category_tree(site)
    root_category_list = list(category for category in category_list if category.parent == 0)
    root_category_id_list = list(category.id for category in root_category_list)
    for category in root_category_list:
        template_dict[category.title] = []
        template_resume[category.id] = []
    post_list = query_recent_posts(site, lookup_posts)
    for post in post_list:
        #print post.template_eligible(), " || ", post.template_dict()
        if post.template_eligible():
            if post.category.id not in root_category_id_list:
                post.append_root_category(category_by_id[post.category.id].root)
                category_title = post.root_category.title
                category_id = post.root_category.id
            else:
                category_title = post.category.title
                category_id = post.category.id
            try:
                if len(template_dict[category_title]) < max_post_per_cat:
                    template_dict[category_title].append(post.template_dict(short_text_max_char))
                    template_resume[category_id].append(post.id)
            except:
                pass
    return [template_dict, template_resume]

#query_recent_posts('mag', 1)
#template_dict, template_resume = template_package('special', 100, 4, 200)
#print template_resume

def post_dict_to_xml(post_dict, xml_object, xml_doc):
    cpt_content = 0
    for key, value in post_dict.iteritems():
        xml_object.append(xml_doc.new_tag(str(key)))
        xml_container = xml_object.contents[cpt_content]
        xml_formatted_value = "<![CDATA[" + str(value) + "]]>"
        xml_container.append(xml_doc.new_string(xml_formatted_value))
        cpt_content += 1

def build_xml_newsletter(template_pack, xml_doc):
    xml_doc.append(xml_doc.new_tag("newsletter"))
    xml_newsletter = xml_doc.newsletter
    cpt_content = 0
    for category, post_list in template_pack.iteritems():
        if post_list:
            xml_newsletter.append(xml_doc.new_tag(str(category).replace(" ", "-")))
            xml_container = xml_newsletter.contents[cpt_content]
            for cpt_post in range(0, len(post_list)):
                xml_container.append(xml_doc.new_tag("item"))
                xml_post = xml_container.contents[cpt_post]
                post_dict_to_xml(post_list[cpt_post], xml_post, xml_doc)
            cpt_content += 1
    return xml_doc

def build_xml_feed(site, lookup_posts = 50, max_post_per_cat = 4, short_text_max_char = 200):
    template_pack, ___ = template_package(site, lookup_posts = lookup_posts, \
                                          max_post_per_cat = max_post_per_cat, \
                                          short_text_max_char = short_text_max_char)
    #for k in new_structure.iterkeys():
    #    print k, new_structure[k]
    xml_doc = build_xml_newsletter(template_pack, BeautifulSoup(features='xml'))
    xml_feed = xml_doc.prettify()
    xml_feed = xml_feed.replace("&lt;!", "<!").replace("]&gt;", "]>").replace("&lt;p&gt;", "").replace("&lt;/p&gt;", "")
    return xml_feed

print build_xml_feed('special')
#print query_wp_json_recent_posts('special', 10)
#print query_recent_posts('special', 10, True)
#print template_package('special', 100)