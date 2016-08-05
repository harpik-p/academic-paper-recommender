# Academic Paper Recommender
Galvanize Data Science Immersive Program - Capstone Project - Pinar Colak


## Overview
The purpose of this project is to develop a web-based academic paper
recommendation system for researchers so that they can stay on top of 
their field without being overwhelmed by information overload. In some 
fields, such as biomedical research, there are roughly 3,000-5,000 papers 
published on a single day over 11,000 journals. As such, it is not feasible 
to conveniently browse millions of articles to find relevant information 
and articles of interest. 

The system is mostly based on collaborative filtering and graph analysis 
techniques. My aim was to use the abstract and fulltext of papers (if available) 
along with citations (if available) in order to find most relevant papers for a given paper.

## Data Sources
National Institute for Health ([NIH](http://www.ncbi.nlm.nih.gov/pubmed)) 
provides comprehensive API to access the central biomedical metadata 
repository named PubMed. I extracted information from raw XML files of about 
1M papers. The dataset contains full text for only a few million papers 
but metadata (publication year, title, journal name, semantic tags, abstract, 
author and affiliation info) for all papers. In addition, it contains out-going 
citation information for a subset of the papers, which contain a total of 
60M citations. Hence, I focused mostly on abstract, title, journal 
information and citation data.

## Data Preparation
XML source files were parsed into a mySQL database containing two tables 
“paper” and “citation”. Subsequently, this tables are dumped into S3 in 
a format ready to be consumed by Spark accessed through Python shell.

## Modelling
The recommender includes 4 core recommenders utilizing different types of knowledge to come up with recommendations:
* Text similarity based recommender: Finds top-K most similar papers based on TF-IDF based similarity as calculated on abstract and title;
* Semantic similarity based recommender: Finds the most semantically similar papers using manually annotated semantic tags of each paper, which is available for a large portion of the papers;
* Incoming citation based recommender: Finds paper-X such that papers who cited this paper also cited paper-X;
* Outgoing citation based recommender: Finds paper-X such that papers cited by this paper are also cited by paper-X;

Finally, all recommendations are processed by an aggregation algorithm that comes up with the optimal list of most relevant papers. I implemented the recommendation algorithms in Spark using Python, and run them on EMR cluster from AWS.

## Repo Structure
```
├── model
|   |── citations.py (engine making recommendations based on citations)
|   └── similarity.py (engine making recommendations based on tfidf and semantic analysis)
|
├── scraper
|   └── scrape.py (scraping functions for NIH website)
|
├── web_app
|   ├── static (images, CSS and JavaScript files)
|   ├── templates (web-page templates)
|   └── app.py (the Flask application)
```

## Deployment
The algorithms runs in batch mode, meaning that recommendations for each paper are pre-calculated and loaded into “recommendation” table. This table stores 50 recommendations for each of the algorithm as well as 50 recommendations from the aggregation algorithm. The content of this table are accessed and queried through a web-interface. Every biomedical paper has a unique id, called PMID, which stands for PubMed id. In the web-interface the user is expected to supply the PMID of the paper to get recommendations for. Ranked results from each algorithm are presented as well as the final aggregation list in the web interface.

