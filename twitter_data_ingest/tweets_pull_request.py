import requests
import json
import tweet_parameters as tp
import datetime
import logging


# Basic tweets search function with query and tweet fields only
def search_twitter(query, tweet_fields, bearer_token=tp.BEARER_TOKEN):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}

    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}".format(
        query, tweet_fields
    )
    response = requests.request("GET", url, headers=headers)

    print(response.status_code)

    if response.status_code != 200:
        raise Exception(response.status_code, response.text)

    print("tweets data pull successful")
    return response.json()


# twitter recent search function with query and other expansion fields of place, user and author
def search_twitter_deep(query, tweet_fields, place_fields="", user_fields="", expansion_list="", bearer_token=tp.BEARER_TOKEN):
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


def main():
    # Python main method to drive the script functions

    # Defining tweets query
    query = "Sydney Melbourne"
    # tweets basic fields
    tweet_fields = "tweet.fields=text,created_at,author_id,geo,id"
    # Tweets place and user fields
    place_fields = "place.fields=full_name,place_type"
    user_fields = "user.fields=username"
    expansions_list = "author_id,geo.place_id"

    # Call twitter search function based on user's input at command prompt
    i = input("Please enter for basic search or d for deep search: ")

    if i == "":
        # Call to basic search function
        json_res = search_twitter(query, tweet_fields)
    else:
        # Call to advance search function
        json_res = search_twitter_deep(
            query, tweet_fields, place_fields, user_fields, expansions_list)

    # Writing twitter data to log file in json format
    json_data = json.dumps(json_res, indent=4, sort_keys=True)
    logging.info('%s : %s', datetime.date.today, json_data)
    print("Data written in log file successfully")


if __name__ == '__main__':
    # configure log file
    fname = 'logging/tweet_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.log'
    logging.basicConfig(filename=fname, filemode='w',
                        level=logging.INFO)
    main()
