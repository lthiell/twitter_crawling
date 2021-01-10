# Twitter Crawler / Sampler

This tools allows the crawling and sampling from big amounts of Tweets from Twitter. It is based on the tool [snscrape](https://github.com/JustAnotherArchivist/snscrape). 

## Installation
First, you need to install snscrape on your local machine. For this tool, a forked version from snscrape with some minor changes is suggested. Install it like this:

>pip install git+https://github.com/lthiell/snscrape

Additionally, you need pandas installed, e.g.:
>pip install pandas

## Configuration
The crawler can be configured with the in/settings.cfg with the following options:

rel_amount_most_retweets: The proportion of the Tweets you want to sample ordered ascending by number of retweets. Range from 0 to 1.

rel_amount_random: The proportion of random Tweets you want to sample. Range from 0 to 1.

keep_original_files: Decides whether the not sampled original files from snscrape are kept. Attention: Keeping the files needs potentially a lot of disk space. True / False.

max_amount_per_timeframe: The maximum of amounts per timeframe (=month) to be crawled.

## Usage
Enter your hashtags (without #) in the in/hashtags.txt file, one hashtag per line.
Enter your keywords in the in/keywords.txt file, one keyword per line.
Run the script src/concurrent_twitter_crawler.py.