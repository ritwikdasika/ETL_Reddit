'''The purpose of my ETL Project is to demonstrate my ability to use packages
to extract data from a reddit page (or a twitter page), transform each
reddit thread into a list of dictionaries consisting of the title, author,
upvotes, and number of comments under each thread listed on the
r/COVID19 reddit page, sorted by the number of comments and
upvote ratio then load the data into a .csv file.

We are only allowed to use
requests and unidecode package.

All code is in accordance with PEP-8 format.
'''
import requests
from unidecode import unidecode
import csv


def reddit_extract():  # Extraction
    ''' Returns data from Reddit JSON page. '''
    try:
        subreddit = input("Enter a subreddit (i.e. r/worldnews) \n")
        if not subreddit:
            raise ValueError
        else:
            reddit = 'https://www.reddit.com/'
            url = reddit + subreddit + '.json'
    except ValueError:
        url = 'https://www.reddit.com/r/worldnews.json'

    head = {'user-agent': "Ritwik Dasika Demo App.0.0.1"}
    rawdata = requests.get(url, headers=head).json()
    # Returns items within key children within key data within object rawdata
    # This return statement helps us iterate through the JSON file
    # without having to count the number
    # of children in the JSON file. (See the for loop in reddit_transform)
    return rawdata['data']['children']


def reddit_transform(dataobj):  # Transformation
    '''Transforms raw data into a list of dictionaries.'''
    count = 0  # count to iterate through indices
    collection = []  # collection list
    for dict in dataobj:  # We can now iterate by dictionary in key children
        temp = {}  # create temporary dict
        temp['title'] = dataobj[count]['data']['title']
        temp['author'] = dataobj[count]['data']['author']
        temp['upvote-ratio'] = dataobj[count]['data']['upvote_ratio']
        temp['ups'] = dataobj[count]['data']['ups']
        temp['numcomments'] = dataobj[count]['data']['num_comments']
        # append the temporary dict into collection.
        collection.append(temp)
        # increase count by 1, the temporary dictionary is
        # cleared as it goes back to temp = {}
        # However, the dictionary has already been added
        # to the list. So we have lost no data.
        count += 1
    # The following line sorts list by criteria in descending order.
    collection = sorted(collection, key=lambda i:
                        (i['numcomments'], i['upvote-ratio']), reverse=True)
    return collection


def reddit_load_to_csv(dataobj):  # Loading
    ''' The purpose of this function is to load our list collection into a csv
    We want to structure the data so that it is clean and can be used'''
    keys = dataobj[0].keys()
    # the following statements organize the data by row
    with open('reddit-thread.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(dataobj)

reddit_load_to_csv(reddit_transform(reddit_extract()))
