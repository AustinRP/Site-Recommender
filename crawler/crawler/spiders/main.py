# -*- coding: utf-8 -*-
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from urlparse import urlparse

class MainSpider(CrawlSpider):
    name = 'main'
    start_urls = ['http://google.com/']

    rules = (
        Rule(LinkExtractor(), callback='parse_full_text', follow=True),
    )

    def parse_full_text(self, response):
        domain = self.get_domain(response.url)
        full_text = ' '.join(sel.select("//body//text()").extract()).strip()
        i = {
            'domain': domain,
            'full_text': full_text
        }
        return i

    # Source: http://stackoverflow.com/questions/9626535/
    def get_domain(self, url):
        parsed_uri = urlparse(url)
        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        return domain
