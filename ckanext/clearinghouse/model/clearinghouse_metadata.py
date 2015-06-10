# -*- coding: utf-8 -*-
from lxml import etree

import hashlib
import logging


log = logging.getLogger(__name__)

class ClearinghouseXmlObject(object):
    docs = {}  # id, docuement

    def __init__(self, str):
        assert (str is not None), 'Must provide some XML in one format or another'
        self.__parse(str)

    def __parse(self, str):
        '''Add all the Documents found in the response'''

        xml = etree.fromstring(str)

        for doc in xml.findall('Document'):
            cdoc = ClearinghouseDocument(xml=doc)
            self.docs[cdoc.get_id()] = cdoc


class ClearinghouseDocument(object):

    def __init__(self, xml=None, str=None):
        assert (str or xml is not None), 'Must provide some XML in one format or another'
        self.xml = xml if xml is not None else etree.fromstring(str)

    def get_id(self):
        title = self.get_title()
        m = hashlib.md5()
        m.update(title.encode('utf8', 'ignore'))
        return m.hexdigest()

    def get_title(self):
        return self.xml.findtext('Title').replace("\n", " ").replace("\r", " ").strip()

    def get_abstract(self):
        return self.xml.findtext('Description')

    def get_author(self):
        return self.xml.findtext('Author')

    def get_publisher(self):
        return self.xml.findtext('Publisher')

    def get_publication_place(self):
        return self.xml.findtext('PublicationPlace')

    def get_publication_date(self):
        return self.xml.findtext('Month') + " " + self.xml.findtext('Year')

    def get_keywords(self):
        '''Returns a list of strings '''
        keywords = self.xml.findtext('Keywords')
        return keywords.split(",") if keywords else list()

    def get_topics(self):
        '''Returns a list of strings '''
        topics = self.xml.findtext('Topic')
        return topics.split(",") if topics else list()

    def get_hyperlink(self):
        return self.xml.findtext('HyperLink')

    def get_text_type(self):
        return self.xml.findtext('TextType')

    def get_paper_type(self):
        return self.xml.findtext('PaperType')

    def tostring(self):
        return etree.tostring(self.xml)


