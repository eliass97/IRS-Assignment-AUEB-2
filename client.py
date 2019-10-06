import os
import time
from elasticsearch5 import Elasticsearch

es = Elasticsearch()
# Set the path for the new files
path = "Collection_2/"
# Set the path for the queries txt
Qfile = "testingQueries.txt"
QfileNew = "testingQueriesNew.txt"
counter = 0
mode = "not_all"
# Set mode to 'all' if you want to read the files from Collection_2 and upload them to elasticsearch
# Set the mode to 'other' if you have already uploaded the new data and you want to execute the queries part only
part = "c"
# Set part to 'a', b' or 'c' according to the part of phase 2 you want to execute
# Note that part a will be connected to the previous data base (test)
# While part b and c will be connected to the new one (test2)
# Therefore before setting part to 'b' or 'c' make sure you have already run mode for 'all'
if mode == "all":
    # Creates the index form
    # Sets the english analyzer to elasticsearch before inserting the data
    es.indices.create(
        index='test2',
        ignore=400,
        body={
            'mappings': {
                'project': {
                    'properties': {
                        'rcn': {
                            'type': 'integer'
                        },
                        'text': {
                            'type': 'string',
                            'analyzer': 'english',
                            'search_analyzer': 'english'
                        }
                    }
                }
            }
        }
    )
    # Opens every single file in the folder
    for file in os.listdir(path):
        # Creates the full path
        filename = os.path.join(path, file)
        # Opens the file
        with open(str(filename), 'r', encoding='utf-8') as fd:
            # Reads the first line - files in team_2 have 0 or 1 lines each
            line = fd.readline()
        counter += 1
        package = {
            # Keeps the number from ######.txt
            'rcn': str(file[:-4]),
            # Removes the space at the beginning of the text
            'text': str(line[1:])
        }
        if counter % 100 == 0:
            print("Files processed: " + str(counter))
        # Uploads the package-json to elasticsearch using as id the unique name of the file
        es.index(index='test2', doc_type='project', id=package['rcn'], body=package)
    print("Files processed: "+str(counter))
    # Closes the indices - changes the settings to TF-IDF - opens the indices
    es.indices.close(index='test2')
    es.indices.put_settings(
        index='test2',
        body={
            'index': {
                'similarity': {
                    'default': {
                      'type': 'classic'
                    }
                }
            }
        }
    )
    es.indices.open(index='test2')
    time.sleep(1)

if part == "a" or part == "b":
    # First we need to read the files given in the PDF and unite them in a single query txt
    fd = open(QfileNew, 'w', encoding='utf-8')
    fd2 = open(path + '193378.txt', 'r', encoding='utf-8')
    line = fd2.readline()
    fd.write("Q01"+line+"\n")
    fd2.close()
    fd2 = open(path + '213164.txt', 'r', encoding='utf-8')
    line = fd2.readline()
    fd.write("Q02" + line + "\n")
    fd2.close()
    fd2 = open(path + '204146.txt', 'r', encoding='utf-8')
    line = fd2.readline()
    fd.write("Q03" + line + "\n")
    fd2.close()
    fd2 = open(path + '214253.txt', 'r', encoding='utf-8')
    line = fd2.readline()
    fd.write("Q04" + line + "\n")
    fd2.close()
    fd2 = open(path + '212490.txt', 'r', encoding='utf-8')
    line = fd2.readline()
    fd.write("Q05" + line + "\n")
    fd2.close()
    fd2 = open(path + '210133.txt', 'r', encoding='utf-8')
    line = fd2.readline()
    fd.write("Q06" + line + "\n")
    fd2.close()
    fd2 = open(path + '213097.txt', 'r', encoding='utf-8')
    line = fd2.readline()
    fd.write("Q07" + line + "\n")
    fd2.close()
    fd2 = open(path + '193715.txt', 'r', encoding='utf-8')
    line = fd2.readline()
    fd.write("Q08" + line + "\n")
    fd2.close()
    fd2 = open(path + '197346.txt', 'r', encoding='utf-8')
    line = fd2.readline()
    fd.write("Q09" + line + "\n")
    fd2.close()
    fd2 = open(path + '199879.txt', 'r', encoding='utf-8')
    line = fd2.readline()
    fd.write("Q10" + line)
    fd2.close()
    fd.close()
    # And now the testingQueries.txt has all the needed questions for the base
    # Opens the queries file and reads each line
    fd = open(QfileNew, 'r', encoding='utf-8')
    # The file where we write the results for a size equal to 20
    if part == "a":
        fd2 = open("es_results_20_a.txt", 'w', encoding='utf-8')
    elif part == "b":
        fd2 = open("es_results_20_b.txt", 'w', encoding='utf-8')
    line = fd.readline()
    # For each line-query it sends a search request to elasticsearch
    while line:
        # Removes the tag (Q#) from the line-query
        # We cut 4 slots for the tag
        tag = line[:4]
        tag = tag[:3]
        line = line[4:]
        # We need 21 results
        if part == "a":
            result = es.search(index='test', doc_type='project', body={'query': {'match': {'text': line}}, 'size': 21})
        elif part == "b":
            result = es.search(index='test2', doc_type='project', body={'query': {'match': {'text': line}}, 'size': 21})
        print(tag + ":")
        # Removes the first hit
        remove_first = 1
        counter = 0
        for hit in result['hits']['hits']:
            # The first element is not written in the txt file (it has the same text as the query)
            if remove_first == 1:
                remove_first = 0
            else:
                counter += 1
                # Writes the result
                fd2.write(tag + " Q0 " + str(hit['_id']) + " " + str(counter) + " " + str(hit['_score']) + " " + str(hit['_index']) + "\n")
                print(hit['_source']['rcn'])
        print("\n")
        line = fd.readline()
    fd.close()
    fd2.close()
elif part == "c":
    # Opens the queries file and reads each line
    fd = open(Qfile, 'r', encoding='utf-8')
    # The file where we write the results for a size equal to 20
    fd2 = [None]*3
    for i in range(0,3):
        fd2[i] = open("es_results_20_c_test_"+str(1+i)+".txt", 'w', encoding='utf-8')
    line = fd.readline()
    # For each line-query it sends a search request to elasticsearch
    remove_special = 1
    while line:
        # Removes the tag (Q#) from the line-query
        # The first line has special characters that need to be removed (we cut 5 slots for the tag)
        # The rest of the lines have no special characters (we cut 4 slots for the tag)
        if remove_special == 1:
            tag = line[1:4]
            line = line[5:]
            remove_special = 0
        else:
            tag = line[:3]
            line = line[4:]
        # Creates the packages for the query (3 tests)
        package = [None]*3
        package[0] = {
            'query': {
                'more_like_this': {
                    'fields': ['text'],
                    'like': line,
                    'max_query_terms': 40,
                    'min_term_freq': 5,
                    'min_doc_freq': 10,
                    'max_doc_freq': 1000,
                    'minimum_should_match': '30%'
                }
            },
            'size': 21
        }
        package[1] = {
            'query': {
                'more_like_this': {
                    'fields': ['text'],
                    'like': line,
                    'max_query_terms': 25,
                    'min_term_freq': 2,
                    'min_doc_freq': 5,
                    'max_doc_freq': 500,
                    'minimum_should_match': '30%'
                }
            },
            'size': 21
        }
        package[2] = {
            'query': {
                'more_like_this': {
                    'fields': ['text'],
                    'like': line,
                    'max_query_terms': 10,
                    'min_term_freq': 1,
                    'min_doc_freq': 1,
                    'max_doc_freq': 0,
                    'minimum_should_match': '30%'
                }
            },
            'size': 21
        }
        result = [None]*3
        for i in range(0,3):
            # The results of each test
            result[i] = es.search(index='test', doc_type='project', body=package[i])
            print(tag + " - 20 - Test "+str(i+1)+":")
            # Removes the first hit (it's the same as the query text)
            remove_first = 1
            counter = 0
            for hit in result[i]['hits']['hits']:
                # The first element is not written in the txt file (it has the same text as the query)
                if remove_first == 1:
                    remove_first = 0
                else:
                    counter += 1
                    # Writes the result
                    fd2[i].write(
                        tag + " Q0 " + str(hit['_id']) + " " + str(counter) + " " + str(hit['_score']) + " " + str(
                            hit['_index']) + "\n")
                    print(hit['_source']['rcn'])
            print("\n")
        line = fd.readline()
else:
    print("Variable 'part' was not set correctly!")
