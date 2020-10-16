#!/usr/bin/env python
from collections import defaultdict
import json
import sys
import gzip
import pandas as pd
import string


def read_tweets(filename):
    """
    Given a filename, parses out the date, time, username, and name of user
    who was retweeted (if applicable)
    """
    tweet_dict = defaultdict(list)
    with gzip.open(filename, 'r') as f:
        line = f.readline()
        while line:
            json_data = json.loads(line)
            if "delete" not in json_data and "status_withheld" not in json_data:
                date_entry = json_data['created_at']
                poster = json_data['user']['name']
                text = json_data['text']

                if 'retweeted_status' in json_data:
                    retweeted_name = json_data['retweeted_status']['user']['name']
                elif 'retweeted_status' not in json_data:
                    retweeted_name = "NA"
                time_stamp = date_entry[11:19]
                day_month_date = date_entry[:10]

                # Bucket times
                hours_minutes = round_seconds(time_stamp)
                generalized_time = bucket_time(hours_minutes)

                # Add values to dictionary
                tweet_dict["user"].append(parse_name(poster))
                tweet_dict["calendar_date"].append(day_month_date)
                tweet_dict["time"].append(generalized_time)
                if retweeted_name != "NA":
                    tweet_dict["retweet_bin"].append(1)
                else:
                    tweet_dict["retweet_bin"].append(0)
                tweet_dict["retweeted_user"].append(parse_name(retweeted_name))
                tweet_dict["tweet_copy"].append(text)

            # Advance to next tweet
            line = f.readline()
            line = f.readline()

    # return dictionary
    return tweet_dict


def round_seconds(time):
    """
    Takes in time string, rounds seconds to the nearest minute
    and returns string of hours and minutes HH:MM
    """
    seconds = int(time[-2:])
    minutes = int(time[3: -3])
    if seconds >= 30:
        minutes += 1
    converted_time = time[:3] + str(minutes)

    return converted_time


def bucket_time(time):
    """
    Rounds time of tweet to nearest 15 minute bucket
    """
    minutes = int(time[3:])
    hours = int(time[:2])
    if 0 <= minutes < 7.5:
        rounded_minutes = "00"
    elif 7.5 < minutes < 22.5:
        rounded_minutes = "15"
    elif 22.5 < minutes < 37.5:
        rounded_minutes = "30"
    elif 37.5 < minutes < 52.5:
        rounded_minutes = "45"
    else:
        hours += 1
        rounded_minutes = "00"
    rounded_time = str(hours) + ":" + rounded_minutes

    return rounded_time


def parse_name(name):
    # remove non-alphanumeric characters
    name = name.lower()
    # split on whitespace
    name_list = name.split()
    return name_list


def assign_gender(names, female_df, male_df):
    gender_list = []
    for name in names:
        if len(female_df.loc[female_df['name'] == name]) > 0 and \
                len(male_df.loc[male_df['name'] == name]) > 0:
            female_count = int(female_df.loc[female_df['name'] == name]['count'])
            male_count = int(male_df.loc[male_df['name'] == name]['count'])
            if female_count >= male_count:
                gender_list.append("female")
            elif male_count > female_count:
                gender_list.append("male")
        elif len(female_df.loc[female_df['name'] == name]) > 0:
            gender_list.append("female")
        elif len(male_df.loc[male_df['name'] == name]) > 0:
            gender_list.append("male")
        else:
            gender_list.append("other")
    if len(gender_list) == 0:
        return "other"
    elif len(gender_list) <= 2:
        # assume that it is the first name
        return gender_list[0]
    elif len(gender_list) > 2:
        female_num = 0
        male_num = 0
        # tally genders, select the most prevalent
        for gender in gender_list:
            if gender == "female":
                female_num += 1
            elif gender == "male":
                male_num += 1
        if female_num == 0 and male_num == 0:
            return "other"
        elif female_num >= male_num:
            return "female"
        elif male_num > female_num:
            return "male"


def main():
    # Specific to the terminal call python3 parse_tweets.py filename.gz
    filename = sys.argv[1]
    tweet_dict = read_tweets(filename)
    female_df = pd.read_csv("female_names.tsv", sep="\t")
    # assuming no twitter user is older than 100, sorted by name popularity
    filtered_fem_df = female_df.where(female_df['year'] >= 1920) \
        .drop(columns=['year']) \
        .groupby(['name']) \
        .sum() \
        .sort_values("count", ascending=False) \
        .reset_index()
    male_df = pd.read_csv("male_names.tsv", sep="\t")
    # assuming no twitter user is older than 100, sorted by name popularity
    filtered_male_df = male_df.where(male_df['year'] >= 1920) \
        .drop(columns=['year']) \
        .groupby(['name']) \
        .sum() \
        .sort_values("count", ascending=False) \
        .reset_index()
    filtered_fem_df['name'] = filtered_fem_df['name'].str.lower()
    filtered_male_df['name'] = filtered_male_df['name'].str.lower()
    for user_names in tweet_dict["user"]:
        tweet_dict['gender'].append(assign_gender(user_names, filtered_fem_df, filtered_male_df))
    for retweeted_names in tweet_dict["retweeted_user"]:
        if len(retweeted_names) > 0 and retweeted_names[0] == 'na':
            tweet_dict['retweeted_gender'].append('na')
        else:
            tweet_dict['retweeted_gender'].append(assign_gender(retweeted_names, filtered_fem_df, filtered_male_df))
    outfile = filename[:-2] + "csv"
    pd.DataFrame(tweet_dict).to_csv(outfile, index=False)


if __name__ == "__main__":
    main()

