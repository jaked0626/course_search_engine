# CS122: Course Search Engine Part 1
#
# Chris Johnson, Jake Underland
#

import re
import util
import bs4
import queue
import json
import sys
import csv

INDEX_IGNORE = set(['a', 'also', 'an', 'and', 'are', 'as', 'at', 'be',
                    'but', 'by', 'course', 'for', 'from', 'how', 'i',
                    'ii', 'iii', 'in', 'include', 'is', 'not', 'of',
                    'on', 'or', 's', 'sequence', 'so', 'social', 'students',
                    'such', 'that', 'the', 'their', 'this', 'through', 'to',
                    'topics', 'units', 'we', 'were', 'which', 'will', 'with',
                    'yet'])


def make_soup(url):
    '''
    Given a url, return the soup object and request object of this url
    Input: url (a string)
    Output: the soup object and request object
    '''
    request = util.get_request(url)
    if request:
        text = util.read_request(request)
        soup = bs4.BeautifulSoup(text, 'html5lib')
        
    return soup, request


def linked_urls(soup, starting_url, queue=queue.Queue()):
    '''
    Inputs:
        soup: Soup object
        queue: queue object
    Outputs:
        links: queue object containing all of the links in order
    '''
    links = queue
    for link in soup.find_all('a'):
        if link.has_attr("href"):
            relative_url = link['href']
            linked_url = util.convert_if_relative_url(starting_url, relative_url)
            filtered_link = util.remove_fragment(linked_url)
            links.put(filtered_link)

    return links


def register_words(dic, text, coursetitles):  
    '''
    Takes a dictionary mapping course ids (values) to words (keys), 
    a body of text, and the coursetitles associated with that text and 
    and modifies the dictionary to include each word in the text and 
    the corresponding coursetitles. 
    Inputs: 
        dic (dict): dictionary for course word pairs
        text (str): body of text
        coursetitles: list of course id(s) associated with the text
    '''
    matches = re.findall("[a-zA-Z][a-zA-Z0-9]*", text)
    for word in matches:
        if word.lower() not in INDEX_IGNORE:
            if dic.get(word.lower()):
                for course in coursetitles:
                    if course not in dic[word.lower()]:
                        dic[word.lower()].append(course)
            else:
                dic[word.lower()] = list(coursetitles)


def find_course_names(courseblockmaintag, id_dic):
    '''
    Takes a "courseblock main" or "courseblock subsequence"
    tag and finds the course id associated with that tag. 
    Inputs:
        courseblockmaintag (soup): the soup object for a 
          "courseblock main" tag
        id_dic: dictionary mapping course names to course identifiers
    Outputs:
        identifier_lst: list of course identifiers
    '''
    title_tag = courseblockmaintag.find_all("p", class_="courseblocktitle")[0]
    course_code = re.search("[A-Z]{4}\xa0[0-9]{5}", title_tag.text).group()

    return id_dic.get(course_code.replace("\xa0", " "))


def crawl_soup(soup, index={}, id_dic=json.load(open("course_map.json"))):
    '''
    Goes through soup object (one internet page) and indexes words found
    in that object to given index (dict). 
    Inputs:
        soup: Soup Object 
        index: Dictionary for storing words and courses
        id_dic: Dictionary mapping course names to identifiers
    
    Modifies index passed into it. 
    '''
    main_tags = soup.find_all("div", class_="courseblock main")
    for tag in main_tags:
        sequences = util.find_sequence(tag)
        if sequences:  # if courseblock main is a sequence
            seq_course_codes = [find_course_names(subseq, id_dic) 
                                for subseq in sequences]  # list of course ids
            for ptag in tag.find_all("p", class_=["courseblocktitle", 
                                                  "courseblockdesc"]):
                register_words(index, ptag.text, seq_course_codes)  
            for i, subseq in enumerate(sequences):
                subseq_course_code = [seq_course_codes[i]]  # individual course ids
                for ptag in subseq.find_all("p", class_=["courseblocktitle", 
                                                         "courseblockdesc"]):
                    register_words(index, ptag.text, subseq_course_code)
            
        else:  # if it is not a sequence
            course_code = [find_course_names(tag, id_dic)]
            for ptag in tag.find_all("p", class_=["courseblocktitle", 
                                                  "courseblockdesc"]):
                register_words(index, ptag.text, course_code)


def go(num_pages_to_crawl, course_map_filename, index_filename):
    '''
    Crawl the college catalog and generate a CSV file with an index.
    Inputs:
        num_pages_to_crawl: the number of pages to process during the crawl
        course_map_filename: the name of a JSON file that contains the mapping 
        of course codes to course identifiers
        index_filename: the name for the CSV of the index.
    Outputs:
        CSV file of the index
    '''

    starting_url = ("http://www.classes.cs.uchicago.edu/archive/2015/winter"
                    "/12200-1/new.collegecatalog.uchicago.edu/index.html")
    limiting_domain = "classes.cs.uchicago.edu"
    id_dic = json.load(open(course_map_filename))
    # The following data stores are used to house urls and course-word pairs
    visited_urls = set()
    crawled_urls = []
    num_pages = 0
    links_queue = queue.Queue()
    index = {}

    while num_pages < num_pages_to_crawl:
        if util.is_url_ok_to_follow(starting_url, limiting_domain) and \
           starting_url not in visited_urls:
            page, request = make_soup(starting_url)
            redirected_url = util.get_request_url(request)
            visited_urls.update([starting_url, redirected_url]) #visited urls is a set for the sake of efficiency
            crawled_urls.append(starting_url)
            num_pages += 1
            crawl_soup(page, index, id_dic) # word-course pairs are stored in a dictionary before being transferred to csv
            links_queue = linked_urls(page, redirected_url, links_queue)

        if links_queue.empty():
            break

        starting_url = links_queue.get()
    
    with open(index_filename, mode="w") as csvfile:
        csv_writer = csv.writer(csvfile, delimiter = "|")
        for key in sorted(index): # placing index in alphabetical order
            for value in index[key]:
                csv_writer.writerow([value, key])


if __name__ == "__main__":
    usage = "python3 crawl.py <number of pages to crawl>"
    args_len = len(sys.argv)
    course_map_filename = "course_map.json"
    index_filename = "catalog_index.csv"
    if args_len == 1:
        num_pages_to_crawl = 1000
    elif args_len == 2:
        try:
            num_pages_to_crawl = int(sys.argv[1])
        except ValueError:
            print(usage)
            sys.exit(0)
    else:
        print(usage)
        sys.exit(0)

    go(num_pages_to_crawl, course_map_filename, index_filename)