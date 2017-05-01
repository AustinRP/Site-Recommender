# -*- coding: utf-8 -*-
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import Selector
from scrapy.spiders import CrawlSpider, Rule

from nltk.tokenize import RegexpTokenizer
from nltk.util import trigrams

from urllib.parse import urlparse

class MainSpider(CrawlSpider):
    name = 'main'
    start_urls = ['https://moz.com/top500']
    trigram_counts = dict()
    minimum_trigram_count = 500

    rules = (
        Rule(LinkExtractor(), callback='parse_full_text', follow=True),
    )

    def parse_full_text(self, response):
        domain = self.get_domain(response.url)

        # if domain is finished, just return 
        if self.trigram_counts.get(domain, 0) > 500:
            return

        # Get the set of trigrams found on the page
        sel = Selector(response)
        full_text = ' '.join(sel.xpath("//body//text()").extract()).strip()
        word_list = self.tokenize(full_text)
        trigram_set = self.trigram_set(word_list)

        # Add to the trigram counts
        self.trigram_counts[domain] = self.trigram_counts.get(domain, 0) + len(trigram_set)

        # Save the item
        i = {
            'domain': domain,
            'trigram_set': trigram_set
        }
        return i

    # Source: http://stackoverflow.com/questions/9626535/
    def get_domain(self, url):
        parsed_uri = urlparse(url)
        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        return domain

    # Source: http://stackoverflow.com/a/15555162
    def tokenize(self, full_text):
        tokenizer = RegexpTokenizer(r'\w+')
        word_set = tokenizer.tokenize(full_text)
        return word_set

    # Returns a set of strings where each string is a trigram from the 
    # input word list. 
    def trigram_set(self, word_list):
        trigram_tuples = trigrams(word_list)
        trigram_list = [' '.join(t) for t in trigram_tuples]
        return set(trigram_list)

    # def closed( self, reason ):
    #     # will be called when the crawler process ends
    #     # any code 
    #     # do something with collected data 
    #     for i in self.spider_attr: 
    #         print i
