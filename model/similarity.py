from __future__ import division
import numpy as np
import pickle as pkl
import re

import pyspark as ps

#from nltk.tokenize import word_tokenize
#from nltk.corpus import stopwords
#from nltk.stem.porter import PorterStemmer
from collections import Counter
#from nltk.tokenize import word_tokenize
#from nltk.corpus import stopwords
from sklearn.metrics.pairwise import cosine_similarity
import string


##############################################
# tokenizer for paper_info_rdd
'''
def tokenize(text):
    PUNCTUATION = set(string.punctuation)
    STOPWORDS = set(stopwords.words('english'))

    regex = re.compile('<.+?>|[^a-zA-Z]')
    clean_txt = regex.sub(' ', text)
    tokens = clean_txt.split()
    lowercased = [t.lower() for t in tokens]

    no_punctuation = []
    for word in lowercased:
        punct_removed = ''.join([letter for letter in word if not letter in PUNCTUATION])
        no_punctuation.append(punct_removed)
    no_stopwords = [w for w in no_punctuation if not w in STOPWORDS]

    STEMMER = PorterStemmer()
    stemmed = [STEMMER.stem(w) for w in no_stopwords]
    return [w for w in stemmed if w]
'''


def tokenize(text):
    """ Helper function that tokenizes given text.

    Args:
        text (string): The text containing paper title, abstract and journal name

    Return:
        list: tokenized (removed punctuation and lower cased) text
    """
    PUNCTUATION = set(string.punctuation)
    tokens = text.split()
    lowercased = [t.lower() for t in tokens]

    no_punctuation = []
    for word in lowercased:
        punct_removed = ''.join([letter for letter in word if not letter in PUNCTUATION])
        no_punctuation.append(punct_removed)

    return no_punctuation


# tokenizer for the terms, which are separated by ;, and then clean them
def tokenize_terms(term_text):
    """ Helper function that tokenizes given text.

    Args:
        text (string): The text containing raw terms of the paper

    Return:
        list: tokenized (removed punctuation and lower cased) text
    """
    split_tokens = term_text.split(";")
    tokens = []
    for token in split_tokens:
        tokens.append(token.replace(", ", "_").replace(" ", "_"))
    return tokens


##############################################
def get_tf(word_lst, vocab):
    """ Helper function that calculates tf for each word in a given list of words.

        Args:
            word_lst (list of strings): The tokens for each paper
            vocab (list of strings): The complete vocabulary contained in the corpus

        Return:
            array: tf for each word
    """
    count_of_each_word = Counter(word_lst)
    doc_word_count = len(word_lst)
    return np.array([count_of_each_word[v] / doc_word_count if v in count_of_each_word else 0 for v in vocab])


##############################################
# calculate idf
def calculate_idf(tf_paper_rdd):
    """ Helper function that calculates idf for each paper.

        Args:
            rdd: rdd containing the pmid as the key and tf values as values

        Return:
            float: idf for each pmid
    """
    total_doc_count = tf_paper_rdd.count()
    times_words_in_doc = tf_paper_rdd.map(lambda x: ((np.array(x[1][1]) > 0) + 0)).sum()
    return np.log(total_doc_count / times_words_in_doc)


##############################################
# create reference dictionary for ids and tf-idf vectors to be used for cosine similarities
def get_reference_index(rdd):
    """ Helper function that stores the tf-idf vectors as a dictionary for later use (for cosine similarity)

        Args:
            rdd: rdd containing pmid as keys and tf-idf vectors as values

        Return:
            dictionary: dictionary obtained from the rdd
    """
    ref_keys = rdd.map(lambda x: x[0]).collect()
    ref_values = rdd.map(lambda x: x[1][1]).collect()
    return dict(zip(ref_keys, ref_values))


##############################################
# construct reverse_index (dictionary with words as keys and the papers
# that have that word at least with given frequency)
def build_reverse_index(vocab, reference_tf, freq):
    """ Helper function that builds a dictionary with words as keys and the list of papers containing those words as values

        Args:
            vocab (list of strings): The complete vocabulary contained in the corpus
            reference_tf (dictionary): the reference index built by get_reference_index method
            freq (float): threshold for omitting words that do not appear as much as the given frequency

        Return:
            dictionary: dictionary of words that appear more than the given frequency as keys and the list of
            papers containing those words as values
    """
    reverse_index = {}
    for ix, word in enumerate(vocab):
        reverse_index[word] = []
        for key_item in reference_tf.keys():
            if reference_tf[key_item][ix] > freq:
                reverse_index[word].append(key_item)
    return reverse_index


# get only the related papers for given paper (its tokens)
def get_candidates(token_list, reverse_index):
    """ Helper function that gets the candidates based on the words that are shared

        Args:
            token_list (list of strings): token list for a given paper
            reverse_index (dictionary): the dictionary built by build_reverse_index method

        Return:
            list: the list of papers relevant to the given paper based on overlapping words
    """
    candidates = []
    for token in token_list:
        candidates += (reverse_index[token])
    return list(set(candidates))


# get the 50 top similar papers for given paper info
def get_neighbors(paper_tokens, paper_tfidf, reference_tf, reverse_index):
    """ Helper function that returns the 50 most similar papers for given paper

        Args:
            paper_tokens (list of strings): token list for a given paper
            paper_tfidf (array): tf-idf vector for a given paper
            reference_tf (dictionary): the reference index built by get_reference_index method
            reverse_index (dictionary): the dictionary built by build_reverse_index method

        Return:
            list: the list of papers relevant to the given paper based on cosine similarity
    """
    candidates = get_candidates(paper_tokens, reverse_index)
    distances = []
    for candidate in candidates:
        distance = cosine_similarity(reference_tf[candidate], paper_tfidf)
        distances.append((candidate, distance))
    sorted_distances = sorted(distances, key=lambda x: x[1], reverse=True)
    # omit the first one as it is the paper itself, return the top 50
    neighbors = [pairs[0] for pairs in sorted_distances[1:51]]
    return neighbors

def text_similarity_recommender(paper_rdd):
    """ Main method that uses the helper functions and returns the 50 most similar papers for each pmid based on
    cosine similarity

        Args:
            paper_rdd (rdd): rdd containing the pmid as keys and title, abstract and journal name as values

        Return:
            rdd: rdd containing the pmid as keys and list of most relevant papers as values
    """
    papers_rdd = paper_rdd.map(lambda x: x.split('\t'))
    headers = papers_rdd.first()
    papers_rdd = papers_rdd.filter(lambda x: x != headers)

    # get title, abstract and journal name for each paper
    paper_info_rdd = papers_rdd.map(lambda x: (x[0], (x[2] + " " + x[3] + " " + x[4]).lower()))

    print("====>   Tokenizing...")
    # tokenize paper_info
    token_rdd = paper_info_rdd.map(lambda x: (x[0], tokenize(x[1])))

    print("====>   Calculating tf-idf...")
    # compute tf
    #vocab_papers = token_rdd.flatMap(lambda x: x[1]).distinct().collect()
    temp_vocab = dict(token_rdd.collect())
    vocab_papers = list(set(sum(temp_vocab.values(), [])))
    tf_papers_rdd = token_rdd.map(lambda x: (x[0], (x[1], get_tf(x[1], vocab_papers))))

    # compute idf
    idf_papers = calculate_idf(tf_papers_rdd)

    # compute tf-idf
    tfidf_papers_rdd = tf_papers_rdd.map(lambda x: (x[0], (x[1][0], x[1][1] * idf_papers)))

    print("====>   Building reference index...")
    # build reference index
    reference_tf_papers = get_reference_index(tf_papers_rdd)

    print("====>   Building reverse index...")
    # build reverse indices
    reverse_index_papers = build_reverse_index(vocab_papers, reference_tf_papers, 0.01)

    print("====>   Getting neighbors...")
    # get top 50 similar papers rdd
    neighbors_papers_rdd = tfidf_papers_rdd.map(lambda x: (x[0], (get_neighbors(x[1][0], x[1][1],
                                                                                reference_tf_papers,
                                                                                reverse_index_papers))))
    return neighbors_papers_rdd


def semantic_recommender(paper_rdd):
    """ Main method that uses the helper functions and returns the 50 most similar papers for each pmid based on
    cosine similarity

        Args:
            paper_rdd (rdd): rdd containing the pmid as keys and raw terms as values

        Return:
            rdd: rdd containing the pmid as keys and list of most relevant papers as values
    """
    papers_rdd = paper_rdd.map(lambda x: x.split('\t'))

    headers = papers_rdd.first()  # this is a list for reference
    papers_rdd = papers_rdd.filter(lambda x: x != headers)

    # get terms for each paper
    semantic_info_rdd = papers_rdd.map(lambda x: (x[0], (x[5]).lower()))

    print("====>   Tokenizing...")
    # tokenize the terms
    semantic_token_rdd = semantic_info_rdd.map(lambda x: (x[0], tokenize_terms(x[1])))

    print("====>   Calculating tf-idf...")
    # compute tf
    #vocab_terms = semantic_token_rdd.flatMap(lambda x: x[1]).distinct().collect()
    temp_vocab = dict(semantic_token_rdd.collect())
    vocab_terms = list(set(sum(temp_vocab.values(), [])))
    tf_terms_rdd = semantic_token_rdd.map(lambda x: (x[0], (x[1], get_tf(x[1], vocab_terms))))

    # compute idf
    idf_terms = calculate_idf(tf_terms_rdd)

    # compute tf-idf
    tfidf_terms_rdd = tf_terms_rdd.map(lambda x: (x[0], (x[1][0], x[1][1] * idf_terms)))

    print("====>   Building reference index...")
    # build reference indices
    reference_tf_terms = get_reference_index(tf_terms_rdd)

    print("====>   Building reverse index...")
    # build reverse indices
    reverse_index_terms = build_reverse_index(vocab_terms, reference_tf_terms, 0.05)

    print("====>   Getting neighbors...")
    # get top 50 similar papers rdd
    neighbors_terms_rdd = tfidf_terms_rdd.map(lambda x: (x[0], (get_neighbors(x[1][0], x[1][1],
                                                                              reference_tf_terms,
                                                                              reverse_index_terms))))

    return neighbors_terms_rdd


def rdd_flattener(key, values, algo_name):
    """ A helper function that takes pmid as key and a list of pmid's as value, and flattens it so that each pmid in the
    list is matched with the key.

    Args:
        key (string): pmid number
        values (list of strings): list of pmid numbers
        algo_name (string): string describing which algorithm is used to obtain the given list (either "tfidf"
        or "semantic").

    Return:
        list: flatten list consisting of key (pmid), value (pmid), rank(order of relevance) and algorithm name
    """
    flatten_list = []
    for rank, value in enumerate(values):
        new_item = [str(key), str(value), str(rank+1), algo_name]
        flatten_list.append(new_item)
    return flatten_list


if __name__ == '__main__':
    sc = ps.SparkContext('local[4]')
    similarity_rdd = sc.textFile('../data/processed/metadata.tab')

    print("Text similarity recommender...")
    papers_rdd = text_similarity_recommender(similarity_rdd)
    papers_rdd = papers_rdd.map(lambda x: rdd_flattener(x[0], x[1], "tfidf")).flatMap(lambda x: x)
    papers_rdd.saveAsTextFile("../data/tfidf")

    print("Semantic similarity recommender...")
    terms_rdd = semantic_recommender(similarity_rdd)
    terms_rdd = terms_rdd.map(lambda x: rdd_flattener(x[0], x[1], "semantic")).flatMap(lambda x: x)
    terms_rdd.saveAsTextFile("../data/semantic")
