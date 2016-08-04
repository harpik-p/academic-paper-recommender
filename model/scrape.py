from Bio import Entrez
import pandas as pd


def get_mesh_terms(record):
    # mesh_terms_raw
    list_desc = []
    for rec in record['MedlineCitation']['MeshHeadingList']:
        list_desc.append(rec['DescriptorName'])
    return "; ".join(list_desc)


def get_abstract(record):
    return record['MedlineCitation']['Article']['Abstract']['AbstractText'][0]


def get_year(record):
    return record['MedlineCitation']['Article']['Journal']['JournalIssue']['PubDate']['Year']


def get_journal(record):
    return record['MedlineCitation']['Article']['Journal']['Title']


def get_title(record):
    return record['MedlineCitation']['Article']['ArticleTitle']


def get_citations(record):
    citations = []
    for citation in record['MedlineCitation']['CommentsCorrectionsList']:
        citations.append(str(citation['PMID']))
    return citations


def handler(id_number):
    """ This method takes a pmid number and parses the info into different sections.

        Args:
            id_number: string

        Returns:
            Returns two tuples. The first one contains the paper information, such as, abstract, title, etc.
            The second one contains the citation information (outgoing and incoming).
    """
    handle = Entrez.efetch("pubmed", id=id_number, retmode="xml")
    records = Entrez.parse(handle)
    for record in records:
    # each record is a Python dictionary or list.
        id = str(id_number)
        title = get_title(record)[:-1]
        abstract = get_abstract(record)
        terms = get_mesh_terms(record)
        year = get_year(record)
        journal = get_journal(record).encode('ascii','ignore')
        paper_info = (id, year, title, abstract, journal, terms)

        citations = get_citations(record)
        citation_info = (id, citations)

    handle.close()
    return paper_info, citation_info

if __name__ == "__main__":
    #let's return 1000000 papers starting with 23000000 (newer papers)
    papers = []
    citations = []
    for i in xrange(10):
        try:
            paper_info= handler(str(i+23000000))
            papers.append(paper_info)
            citations.append(citation_info)
        except:
            pass

    #reformat the citations so that we can put them into a data frame
    converted_tuples = []
    for citation_info in citations:
        for paper_id in citation_info[1]:
            converted_tuples.append((citation_info[0], paper_id))

    df_papers = pd.DataFrame(papers, columns=['id', 'year', 'title', 'abstract', 'journal_name_raw', 'mesh_terms_raw'])
    df_papers.to_csv('../data/test.tab', index=False, header=True, sep='\t')

    df_citations = pd.DataFrame(converted_tuples, columns=['id_from_paper', 'id_to_paper'])
    df_citations.to_csv('../data/test_citations.tab', index=False, header=True, sep='\t')