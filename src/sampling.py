import pandas as pd


class TwitterCrawlingResult:

    def __init__(self, orig_filename, chunksize=100_000):
        self.orig_filename = orig_filename
        self.chunksize = chunksize
        self.samples = pd.DataFrame()

    def __filter_chunk(self, chunk):
        is_english = chunk['lang'] == "en"
        chunk = chunk[is_english]
        return chunk

    def sample_most_retweets(self, rel_amount):
        with(open(self.orig_filename, "r")) as file:
            for chunk in pd.read_json(file, lines=True, chunksize=self.chunksize):
                chunk = self.__filter_chunk(chunk)
                row_count = len(chunk.index)
                num_samples = int(row_count * rel_amount)
                samples = chunk.sort_values(by="retweetCount", ascending=False).head(n=num_samples)
                self.samples = self.samples.append(samples)
        return self

    def sample_randomly(self, rel_amount):
        with(open(self.orig_filename, "r")) as file:
            for chunk in pd.read_json(file, lines=True, chunksize=self.chunksize):
                chunk = self.__filter_chunk(chunk)
                samples = chunk.sample(frac=rel_amount)
                self.samples = self.samples.append(samples)
        return self

    def to_json(self, output_filename):
        with(open(output_filename, "w")) as file:
            self.samples.to_json(file, orient="records", lines=True)
