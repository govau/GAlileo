# Python script to extract tweets and load into log files
# The code manages volume of data through pagination
# Keep track of tweet records using counter mechanics

import requests
import json
import tweet_parameters as tp
import datetime
import logging
from twitter import *


def write_data(json_res, query):
    # Writing twitter data to log file in json format
    json_data = json.dumps(json_res, indent=4, sort_keys=True)
    logging.info('%s\n twitter query: %s \n %s',
                 datetime.date.today, query, json_data)
    print("Data written in log file successfully")


# Basic tweets search function with query and tweet fields only
def search_twitter(query, tweet_fields, bearer_token=tp.BEARER_TOKEN_OBS):

    tweets_counter = 0
    counter = 1

    headers = {"Authorization": "Bearer {}".format(bearer_token)}

    # api to extract first 100 tweets
    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}".format(
        query, tweet_fields, "max_results=100"
    )
    response = requests.request("GET", url, headers=headers)

    if response.status_code != 200:
        raise Exception(response.status_code, response.text)

    json_res = response.json()
    tweets_counter = json_res['meta']['result_count']

    # registering outputs on-screen and log file
    logging.info("%s tweets data pull successful: page %s",
                 tweets_counter, counter)
    print(tweets_counter, "\ttweets data pull successful: ", counter)
    write_data(response.json(), query)

    # Loop to extract tweets in batch of 100s
    counter = counter+1
    while (json_res['meta']['next_token']):

        next_token = 'next_token=' + json_res['meta']['next_token']
        url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}".format(
            query, tweet_fields, "max_results=100", next_token
        )
        response = requests.request("GET", url, headers=headers)

        if response.status_code != 200:
            raise Exception(response.status_code, response.text)

        json_res = response.json()
        tweets_counter = tweets_counter+json_res['meta']['result_count']

        # registering outputs on-screen and log file
        logging.info("%s tweets data pull successful: page %s",
                     tweets_counter, counter)
        print(tweets_counter, "\ttweets data pull successful:", counter)

        write_data(response.json(), query)
        counter = counter+1
        if counter == 5:
            break


# twitter recent search function with query and other expansion fields of place, user and author for future use
def search_twitter_deep(query, tweet_fields, place_fields="", user_fields="", expansion_list="", bearer_token=tp.BEARER_TOKEN_OBS):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}

    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}&{}".format(
        query, tweet_fields, place_fields, user_fields, expansion_list
    )
    response = requests.request("GET", url, headers=headers)

    print(response.status_code)

    if response.status_code != 200:
        raise Exception(response.status_code, response.text)

    print("tweets data pull successful")
    return response.json()


def tweet_count(query):

    headers = {"Authorization": "Bearer {}".format(tp.BEARER_TOKEN_OBS)}
    search_url = "https://api.twitter.com/2/tweets/counts/recent"

    # Optional params: start_time,end_time,since_id,until_id,next_token,granularity
    query_params = {'query': query, 'granularity': 'day'}

    response = requests.request(
        "GET", search_url, headers=headers, params=query_params)
    # print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def main():
    # Python main method to drive the script functions

    # Defining tweets query
    query = input("Please enter twitter search keywords:")
    # tweets basic fields
    tweet_fields = "tweet.fields=text,created_at,author_id,geo,id"
    # Tweets place and user fields for future use
    place_fields = "place.fields=full_name,place_type"
    user_fields = "user.fields=username"
    expansion_list = "author_id,geo.place_id"

    tweetCount = tweet_count(query)
    logging.info(json.dumps(tweetCount))

    # initialise input query string
    i = ""

    # Call twitter search function based on user's input at command prompt disbaled
    # i = input("Please enter for basic search or d for deep search: ")

    if i == "":
        # Call to basic search function
        search_twitter(query, tweet_fields)
    else:
        # Call to advance search function
        json_res = search_twitter_deep(
            query, tweet_fields, place_fields, user_fields, expansion_list)


if __name__ == '__main__':
    # create and configure log file
    fname = 'logging/tweet_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.log'
    logging.basicConfig(filename=fname, filemode='w',
                        level=logging.INFO)
    main()
