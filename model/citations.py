from __future__ import division
from collections import Counter

import pyspark as ps


def get_outgoing(paper_id, citations_out_dict, citations_in_dict, top=50):
    """ Gets paper-X such that papers cited by given paper are also cited by paper-X

    Args:
        paper_id (string): pmid number
        citations_out_dict (dictionary): complete list of papers with from_paper_id for key and
        to_paper_id as value
        citations_in_dict (dictionary): complete list of papers with to_paper_id for key and
        from_paper_id as value
        top (int): desired number of related numbers. default is 50.

    Return:
        list: list of top 50 relevant papers for the given paper based on outgoing citations
    """
    cited_papers = citations_out_dict[paper_id]
    outgoing_papers = []
    for paper in cited_papers:
        outgoing_papers.extend(citations_in_dict[paper])
    # get the top relevant papers based on how much overlap there is
    dict_outgoing_papers = Counter(outgoing_papers)
    temp_top50 = [pairs[0] for pairs in dict_outgoing_papers.most_common(top+1)]
    # we omit the first paper as it is the given paper
    return temp_top50[1:]


# get paper-X such that papers citing given paper are also citing paper-X
def get_incoming(paper_id, citations_out_dict, citations_in_dict, top=50):
    """ Gets paper-X such that papers citing given paper are also citing paper-X

    Args:
        paper_id (string): pmid number
        citations_out_dict (dictionary): complete list of papers with from_paper_id for key and
        to_paper_id as value
        citations_in_dict (dictionary): complete list of papers with to_paper_id for key and
        from_paper_id as value
        top (int): desired number of related numbers. default is 50.

    Return:
        list: list of top 50 relevant papers for the given paper based on incoming citations
    """
    citing_papers = citations_in_dict[paper_id]
    incoming_papers = []
    for paper in citing_papers:
        incoming_papers.extend(citations_out_dict[paper])
    # get the top relevant papers based on how much overlap there is
    dict_incoming_papers = Counter(incoming_papers)
    temp_top50 = [pairs[0] for pairs in dict_incoming_papers.most_common(top+1)]
    # we omit the first paper as it is the given paper
    return temp_top50[1:]


def get_citations(citations_rdd):
    """ Creates the top relevant papers for each pmid based on outgoing and incoming citations

    Args:
        citations_rdd (rdd): An rdd created by citations table

    Return:
        tuple: The first tuple is the rdd containing the list of top 50 recommendations based on outgoing citations for each pmid number.
        The second tuple is the rdd containing the list of top 50 recommendations based on incoming citations for each pmid number.
    """
    # load the data
    print("Loading the data...")

    # split the data by '\t'
    citations_rdd = citations_rdd.map(lambda x: x.split('\t'))

    # get rid of the header
    headers = citations_rdd.first()
    citations_rdd = citations_rdd.filter(lambda x: x != headers)

    print("Building dictionaries for citations...")
    # complete list of papers for citation for given paper id as key (citation going in and out)
    citations_out_rdd = citations_rdd.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda val1, val2: val1 + val2)
    citations_in_rdd = citations_rdd.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda val1, val2: val1 + val2)

    # turn the rdd's into dictionaries:
    citations_out_dict = dict(citations_out_rdd.collect())
    citations_in_dict = dict(citations_in_rdd.collect())

    print("Getting the top 50 papers...")
    citations_out_top50_rdd = citations_out_rdd.map(lambda x: (x[0], get_outgoing(x[0], citations_out_dict, citations_in_dict)))
    citations_in_top50_rdd = citations_in_rdd.map(lambda x: (x[0], get_incoming(x[0], citations_out_dict, citations_in_dict)))

    return citations_out_top50_rdd, citations_in_top50_rdd


def rdd_flattener(key, values, algo_name):
    """ A helper function that takes pmid as key and a list of pmid's as value, and flattens it so that each pmid in the
    list is matched with the key.

    Args:
        key (string): pmid number
        values (list of strings): list of pmid numbers
        algo_name (string): string describing which algorithm is used to obtain the given list (either "outgoing_citations"
        or "incoming_citations").

    Return:
        list: flatten list consisting of key (pmid), value (pmid), rank(order of relevance) and algorithm name
    """
    flatten_list = []
    for rank, value in enumerate(values):
        new_item = [str(key), str(value), str(rank+1), algo_name]
        flatten_list.append(new_item)
    return flatten_list


if __name__ == '__main__':
    ps.SparkContext.setSystemProperty('spark.executor.memory', '3g')
    sc = ps.SparkContext('local[4]')

    citations_rdd = sc.textFile('../data/processed/citations.tab')
    out_rdd, in_rdd = get_citations(citations_rdd)

    out_rdd = out_rdd.map(lambda x: rdd_flattener(x[0], x[1], "outgoing_citations")).flatMap(lambda x: x)
    in_rdd = in_rdd.map(lambda x: rdd_flattener(x[0], x[1], "incoming_citations")).flatMap(lambda x: x)

    print('====>   Saving the rdds...')
    out_rdd.saveAsTextFile('outgoing_citations')
    in_rdd.saveAsTextFile('incoming_citations')
