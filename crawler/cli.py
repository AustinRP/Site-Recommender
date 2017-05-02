import pickle
from urllib.parse import urlparse
from queue import Queue
import subprocess

from datasketch import MinHash

# from worker import CrawlerWorker
from crawler.spiders.main import MainSpider

def main():
    # load the lsh forest
    lsh_forest = pickle.load(open('../lsh_forest_data_large', 'rb'))
    lsh_forest.index()

    while(True):
        # prompt for user input: domain and number of things to return
        print('Typing "end" as the url will end the program.')
        url = input('Enter a url from the query website: ').strip()

        # if user inputs 'end', break
        if url.lower == 'end':
            break

        input_num = int(input('Enter a number of results to output: '))
        num_results = input_num

        # check if forest already contains the domain
        # if it does, add 1 to input_num (later, we will subtract either 
        # the last result or the one with the matching domain)
        parsed_uri = urlparse(url)
        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        if domain in lsh_forest:
            num_results = num_results + 1

        # call the scraper to get trigram sets from the domain
        proc = subprocess.run(
            args = ['scrapy', 'crawl', '-a', 'start_url={}'.format(url), '-a', 'allowed_domain={}'.format(domain), '-s', r'ITEM_PIPELINES=\{\}', '-o', '-', '-t', 'json', '--nolog', 'main'],
            universal_newlines = True,
            stdout = subprocess.PIPE)
        json_output = proc.stdout

        ## parse json into result_queue

        # populate MinHash with data from result_queue
        result_queue.put({'domain': domain, 'trigram_set': set(['a b c', 'b c d', 'c d e', 'd e f'])})
        print('results queue: {}'.format(result_queue))
        query_minhash = MinHash()
        for item in result_queue:
            for trigram in item['trigram_set']:
                query_minhash.update(trigram.encode('utf-8'))

        # query using the new minhash
        results = lsh_forest.query(query_minhash, num_results)

        # Remove the input domain if it was one of our results.
        if input_num < num_results:
            if domain in results:
                results.remove(domain)
            else:
                results = results[:-1]

        # return the output in a nice format
        print('Most similar sites:')
        if len(results) == 0:
            print('No results found.')
        for i, val in enumerate(results):
            print('{}. {}'.format(i, val))

        print()

if __name__ == '__main__':
    main()