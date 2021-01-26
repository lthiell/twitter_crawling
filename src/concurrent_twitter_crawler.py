import os
from concurrent.futures.thread import ThreadPoolExecutor
from configparser import ConfigParser

from src.sampling import TwitterCrawlingResult
from src.util import read_txt


def read_config(filename):
    parser = ConfigParser()
    parser.read(filename)
    rel_amount_random = parser.getfloat('sampling', 'rel_amount_random')
    rel_amount_retweets = parser.getfloat('sampling', 'rel_amount_most_retweets')
    max_amount_per_timeframe = parser.getint('scraping', 'max_amount_per_timeframe')
    keep_original_files = parser.getboolean('sampling', 'keep_original_files')

    return rel_amount_random, rel_amount_retweets, max_amount_per_timeframe, keep_original_files


def post_process(orig_file_name, output_file_name, random, retweets, keep_orig=False):
    crawling_res = TwitterCrawlingResult(orig_file_name)
    crawling_res \
        .sample_most_retweets(retweets) \
        .sample_randomly(random) \
        .to_json(output_file_name)
    if not keep_orig:
        os.chmod(orig_file_name, 0o777)
        os.remove(orig_file_name)


def scrape_for_key_word(key_word, start_date, end_date, tf_name, max_amount_per_timeframe, rel_amount_random,
                        rel_amount_retweets, keep_original_files):
    print("Started scraping keyword : " + key_word + ", " + start_date + ", " + end_date)
    file_name = "tweets_kw_" + key_word + "_" + tf_name + ".json"
    os.system("snscrape --jsonl --progress --max-results " + str(max_amount_per_timeframe)
              + " --since " + start_date + " twitter-search \""
              + key_word + " until:" + end_date
              + "\" > " + "../tmp/" + file_name)
    print("Finished scraping keyword: " + key_word + ", " + start_date + ", " + end_date)
    print("Starting postprocessing keyword: " + key_word + ", " + start_date + ", " + end_date)
    post_process("../tmp/" + file_name, "../out/sampled/" + file_name, rel_amount_random, rel_amount_retweets,
                 keep_original_files)
    print("Finished postprocessing keyword: " + key_word + ", " + start_date + ", " + end_date)


def scrape_for_hash_tag(hash_tag, start_date, end_date, tf_name, max_amount_per_timeframe, rel_amount_random,
                        rel_amount_retweets, keep_original_files):
    print("Started scraping hashtag: " + hash_tag + ", " + start_date + ", " + end_date)
    file_name = "tweets_ht_" + hash_tag + "_" + tf_name + ".json"
    os.system("snscrape --jsonl --progress --max-results " + str(max_amount_per_timeframe)
              + " --since " + start_date + " twitter-hashtag \""
              + hash_tag + " until:" + end_date
              + "\" > " + "../tmp/" + file_name)
    print("Finished scraping hashtag: " + hash_tag + ", " + start_date + ", " + end_date)
    print("Started postprocessing hashtag: " + hash_tag + ", " + start_date + ", " + end_date)
    post_process("../tmp/" + file_name, "../out/sampled/" + file_name, rel_amount_random, rel_amount_retweets,
                 keep_original_files)
    print("Finished postprocessing hashtag: " + hash_tag + ", " + start_date + ", " + end_date)


def scrape_concurrently(hash_tags, key_words, time_frames, max_amount_per_timeframe, rel_amount_random,
                        rel_amount_retweets, keep_original_files):
    with ThreadPoolExecutor(max_workers=8) as executor:

        for kw in key_words:
            for tf in time_frames:
                executor.submit(scrape_for_key_word, kw, tf[1], tf[2], tf[0], max_amount_per_timeframe,
                                rel_amount_random,
                                rel_amount_retweets, keep_original_files)

        for ht in hash_tags:
            for tf in time_frames:
                executor.submit(scrape_for_hash_tag, ht, tf[1], tf[2], tf[0], max_amount_per_timeframe,
                                rel_amount_random,
                                rel_amount_retweets, keep_original_files)


def main():
    hash_tags = read_txt("../in/hashtags.txt")
    key_words = read_txt("../in/keywords.txt")
    rel_amount_random, rel_amount_retweets, max_amount_per_timeframe, keep_original_files = read_config(
        "../in/settings.cfg")

    time_frames = {
        ("01", "2020-01-01", "2020-01-31"),
        ("02", "2020-02-01", "2020-02-29"),
        ("03", "2020-03-01", "2020-03-31"),
        ("04", "2020-04-01", "2020-04-30"),
        ("05", "2020-05-01", "2020-05-31"),
        ("06", "2020-06-01", "2020-06-30"),
        ("07", "2020-07-01", "2020-07-31"),
        ("08", "2020-08-01", "2020-08-31"),
        ("09", "2020-09-01", "2020-09-30"),
        ("10", "2020-10-01", "2020-10-31"),
        ("11", "2020-11-01", "2020-11-30"),
        ("12", "2020-12-01", "2020-12-31")
    }

    scrape_concurrently(hash_tags, key_words, time_frames, max_amount_per_timeframe, rel_amount_random,
                        rel_amount_retweets, keep_original_files)


main()
