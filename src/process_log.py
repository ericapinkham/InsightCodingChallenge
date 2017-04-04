#!/usr/bin/python3
import pandas as pd
import re
import datetime
import codecs
import time

def function_timer(function):
    """
    function_timer: a decorator for timing the execution of a function.
    """
    def wrapper(*args, **kwargs):
        print('Executing %s...' %(function.__name__))
        t1 = time.time()
        result = function(*args, **kwargs)
        t2 = time.time()
        print("    done in %f seconds." %(t2 - t1))
        return result

    return wrapper

@function_timer
def read_file(file_name):
    """
    read_file: Reads the log file and extracts fields. Performs minimal preprocessing.
    input:
        file_name: The full path of the file to be read.
    """
    # compile a regex pattern for parsing lines in the file
    regex_pattern = re.compile(r'^((.+).+-.+-.+\[(.+)\s([0-9\-]+)]\s"([A-Za-z]+)\s(\S+)\s?(.*)"\s(\d+)\s([0-9\-]+).*\n)')

    # create a list of lists containing all the desired_fields
    list_of_lists = [re.findall(regex_pattern, line)[0] \
        for line in codecs.open(file_name, 'r', 'ascii', errors = 'ignore') \
        if re.findall(regex_pattern, line)]

    # store this in a pandas DataFrame
    headers = ['raw', 'host', 'timestamp', 'timezone', 'method', 'request_uri', 'http_version', 'http_reply_code', 'bytes_used']
    df = pd.DataFrame(list_of_lists, columns = headers)

    # convert the raw timestamp string to a datetime.datetime
    df.timestamp_datetime = df.timestamp.map(lambda x: datetime.datetime.strptime(x, "%d/%b/%Y:%H:%M:%S"))

    # reindex on the new datetime, this makes certain methods easier later
    df.index = df.timestamp_datetime

    # cast the bytes_used column to integer, since we will aggregate it later
    df.bytes_used = df.bytes_used.map(lambda x: int(x.replace('-','0')))

    return df

@function_timer
def most_active_resources(df):
    """
    most_active_resources: top 10 resources by total bytes_used
    input:
        pd.DataFrame df - a pandas dataframe containing all the data we want to process
    """
    df_bytes_used = df \
        .groupby('request_uri') \
        .agg({'bytes_used':{'total_bytes': 'sum'}}) \
        .bytes_used

    # add a column with index values for sorting on multiple columns
    df_bytes_used['request_uri'] = df_bytes_used.index

    # sort and grab the top 10, breaking ties on request_uri lexicographically
    df_bytes_used = df_bytes_used.sort_values(['total_bytes','request_uri'], ascending = [False, True]).head(10)

    #write the top 10 hosts to a file
    output_file = open('./log_output/resources.txt', 'w+')
    for row in df_bytes_used.itertuples():
        output_file.write('%s\n' %(row.request_uri))

    output_file.close()

    return df_bytes_used

@function_timer
def most_active_hosts(df):
    """
    most_active_resources: Top 10 hosts by total activity. This does not take into account blocks performed later.
    input:
        pd.DataFrame df - a pandas dataframe containing all the data we want to process
    """
    df_hosts = df.groupby('host') \
        .agg({'host': {'host_activity': 'count'}}) \
        .host

    # add a column with index values for sorting on multiple columns
    df_hosts['host'] = df_hosts.index

    # sort and grab the top 10, breaking ties on host lexicographically
    df_hosts = df_hosts.sort_values(['host_activity','host'], ascending = [False, True]).head(10)

    #write the top 10 hosts to a file
    output_file = open('./log_output/hosts.txt', 'w+')
    for row in df_hosts.itertuples():
        output_file.write('%s,%d\n' %(row.host, row.host_activity))

    output_file.close()

    return df_hosts

@function_timer
def hour_activity(df):
    """
    hour_activity: Top 10 60 minute intervals of site activity. These are assumed to be disjoint.
        We are taking a gready approach to identifying the 10 most active windows.
    input:
        pd.DataFrame df - a pandas dataframe containing all the data we want to process
    """
    # first perform a rolling count of all activity in the DataFrame.
    # This rolling count outputs the windows end timestamp as the windows
    # identifier.
    df_activity = df[['timestamp','timezone']].copy()
    df_activity['activity'] = pd.DataFrame( \
        df.groupby(df.index.get_level_values(0))['timestamp','timezone'] \
            .agg({'timestamp': {'activity': 'count'}}) \
            .rolling('1h') \
            .sum() \
        )

    # now transform these into disjoint intervals.
    largest_row = df_activity.nlargest(1, 'activity') # because df is chronological, this will put the first occuring max first
    intervals = [{'time_end': largest_row.index[0], 'activity': largest_row.activity[0], 'timezone': largest_row.timezone[0]}]

    for i in range(9):
        # filter the timestamps based on currently used timestamps
        df_activity = df_activity[ \
            (df_activity.index < intervals[-1]['time_end'] - datetime.timedelta(hours = 1)) \
            | (df_activity.index > intervals[-1]['time_end'] + datetime.timedelta(hours = 1)) \
            ]
        largest_row = df_activity.nlargest(1, 'activity')
        try:
            intervals.append({'time_end': largest_row.index[0], 'activity': largest_row.activity[0], 'timezone': largest_row.timezone[0]})
        except (IndexError):
            # this should only happen when testing very small data sets
            pass

    output_file = open('./log_output/hours.txt', 'w+')
    # write these to file
    for e in intervals:
        output_file.write((e['time_end'] - datetime.timedelta(hours = 1)).strftime('%d/%b/%Y:%H:%M:%S') + ' ' \
            + e['timezone'] \
            + ',%d' %(e['activity']) + '\n')

    output_file.close()

    return intervals

class logins():
    """
    logins: a class to contain all the required stuff for tracking and blocking logins
    """
    def __init__(self, df):
        """
        logins.__init__: initialize the object with required attributes.
        """
        self.cur_time = datetime.datetime(1900,1,1)
        self.failed_logins = {}
        self.blocked_hosts = {}

        # open the output file
        self.file = open('./log_output/blocked.txt', 'w+')

        # process the data
        self.process_logins(df)

        # close the file
        self.file.close()

    @function_timer
    def process_logins(self, df):
        """
        process_logins: loops over the DataFrame and process each entry one at a time.
        """
        for row in df.itertuples():
            if row[0] > self.cur_time:
                self.cur_time = row[0]
                self.clear_expired()

            self.authenticate_host(row)

    def authenticate_host(self, row):
        """
        authenticate_host: decides what to do with a log entry.
        """
        # first check to see if they are blocked or not
        if row.host in self.blocked_hosts.keys():
            # regardless of what they are trying to do, block them
            self.file.write(row.raw)
        elif row.request_uri == '/login':
            # they are not blocked
            if row.http_reply_code != '401' and row.host in self.failed_logins.keys():
                # a successful login after 1 or 2 failed login attemts in less than 20 seconds resets the timer
                self.failed_logins.pop(row.host)
            else:
                # a failed login attempt
                try:
                    failures = [timestamp \
                        for timestamp in self.failed_logins.pop(row.host) \
                        if timestamp >= self.cur_time - datetime.timedelta(seconds = 20)]
                except (KeyError):
                    failures = []

                failures.append(row[0])
                if len(failures) >= 3:
                    # block this user
                    self.blocked_hosts[row.host] = self.cur_time
                else:
                    # put this host back in the stack
                    self.failed_logins[row.host] = failures

    def clear_expired(self):
        """
        clear_expired: removes blocked hosts or users being tracked for failed login if they are outside the window.
        """
        failed_logins_expired = [host \
            for host, failures in self.failed_logins.items() \
            if max(failures) < self.cur_time - datetime.timedelta(seconds = 20)]
        for host in failed_logins_expired:
            self.failed_logins.pop(host)

        blocked_hosts_expired = [host \
            for host, time_blocked in self.blocked_hosts.items() \
            if time_blocked < self.cur_time - datetime.timedelta(minutes = 5)]
        for host in blocked_hosts_expired:
            self.blocked_hosts.pop(host)

file_name = './log_input/log.txt'
# file_name = '/home/eric/insight_coding_challenge/insight_testsuite/log_medium.txt'
# file_name = '/home/eric/insight_coding_challenge/insight_testsuite/log_smallish.txt'
# file_name = '/home/eric/insight_coding_challenge/insight_testsuite/log_small.txt'
# file_name = '/home/eric/insight_coding_challenge/insight_testsuite/failed_logins_test.txt'

df = read_file(file_name)

df_bytes_used = most_active_resources(df)

df_hosts = most_active_hosts(df)

activity_windows = hour_activity(df)

logins_object = logins(df)
