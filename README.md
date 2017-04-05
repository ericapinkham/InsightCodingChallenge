# Solution Summary

## General Approach
I had originally attempted this challenge as though I were receiving the data
in real time. To accomplish this, the code would open the file, read and process
each line one at a time, trying to track the four features in real time. This
was very slow.

My second attempt was focused on speed. As such, the required preprocessing
was handled up front. That is to say, the file was loaded into a pandas
dataframe up front. The dataframe was then indexed on a datetime.datetime index,
which facilitated several of pandas' built in methods.

## Dependencies
process_log.py depends on:
* pandas: a dataframe package
* re: a package which provides tools for regex
* datetime: provides nice datetime datatype
* codecs: for more flexible encoding options
* time: for benchmarking

## Details of Implementation
In what follows is my general approach and a summary of design decisions made
for the implementation of the features.

### Feature 1
With the data loaded into a pandas DataFrame as described above, we can simply
choose the 10 records we care about by sorting appropriately.

To create this feature, we can simply count the number of times a given host has
accessed the site, then sort this by total actions taken, breaking ties on host
name lexicographically.

### Feature 2
In implementing this feature, it may be necessary to define what a resource is,
or what do we really want to track. For the purposes of this exercise a
'resource' is anything that uses bandwidth.

Again for this feature, pandas does most of the work for us. It is a simple
process of aggregating and sorting.

### Feature 3
This feature is a bit more interesting than the previous two in terms of design.
The biggest design question to me was whether or not the intervals should be
disjoint. I eventually decided on disjoint time intervals chosen in a greedy
manor. This decision was made to prevent the following problem: if the site
experienced exceptionally large activity during a given 59 minute and 50 second
time window, all 10 intervals could have their beginning in the same 10 seconds.

To create this feature, pandas' rolling method provides a very nice way to get
the total activity in a given hour. With appropriate aggregates, we get the
ending timestamp of each 60 minute window (that ends on a data point). From here
we disjointify these intervals. This is done by choosing the most active 60
minute window, then the next which doesn't overlap and so on.

### Feature 4
The general strategy to this feature was to create dictionaries of users who had
a failed login in the last 20 seconds, and one of users who have been blocked in
the last 5 minutes. These dictionaries are updated every second, and all action
on the site are checked against these dictionaries.

For each entry:

We check if the host is currently blocked, if so we write their failed attempt
to file.

If the host is not currently blocked, we check to see if the record was a login.
If it was a login and it was successful, we clear remove this host from our list
of hosts currently being tracked for failed logins. Otherwise, we append their
their failed login to a list for that user. Once that user's list reaches 3 in
length, we add them to a list of blocked hosts along with a timestamp.

Each second, we clear any expired failed login attempts or blocked hosts.
