# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from datasketch import MinHashLSHForest, MinHash

import os.path

import pickle

class MinHashPipeline(object):
    pickle_filename = '../lsh_forest_data'
    lsh_forest = None
    minhashes = dict()

    # Number of permutation functions to use in each MinHash
    num_perm = 128

    def open_spider(self, spider):
        # Load forest from pickle if the file exists.
        # We do this at the beginning to ensure that time is not
        # wasted in the event that a problem occurs.
        print('-----  Loading Pickle File  -----')
        if os.path.isfile(self.pickle_filename):
            self.lsh_forest = pickle.load(open(self.pickle_filename, 'rb'))
        else:
            self.lsh_forest = MinHashLSHForest(num_perm=self.num_perm)

    def process_item(self, item, spider):
        domain = item['domain']
        print('domain: {}'.format(domain))
        # make a MinHash object or add to an existing one
        if domain not in self.minhashes:
            # Initialize the MinHash
            self.minhashes[domain] = MinHash(num_perm=self.num_perm)

        # Update the MinHash with each trigram
        for trigram in item['trigram_set']:
            self.minhashes[domain].update(trigram.encode('utf-8'))

        return item

    def close_spider(self, spider):
        print('----- Saving to Pickle File -----')
        # populate forest from MinHashes
        for domain, mh in self.minhashes.items():
            self.lsh_forest.add(domain, mh)

        # Save forest to pickle
        pickle.dump(self.lsh_forest, open(self.pickle_filename, 'wb'))
        print('-----     Pickle Saved      -----')
