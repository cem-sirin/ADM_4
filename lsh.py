import itertools

class LSH:

    def __init__(self, buckets, query_signatures, band_size):
        self.buckets = buckets
        self.query_signatures = query_signatures
        self.band_size = band_size

    # Function to finds the other customers are in the exact same bucket as the query user
    def query_intersection(self):

        intersection = {j: set() for j in range(self.query_signatures.shape[1])}
        num_bucket_matches = {j: 0 for j in range(self.query_signatures.shape[1])}

        rows = self.query_signatures.shape[0] // self.band_size

        for i in range(rows):
            for j in range(self.query_signatures.shape[1]):
                bucket = (i, *self.query_signatures[i*self.band_size:(i+1)*self.band_size, j])
                if bucket in self.buckets.keys():
                    intersection[j] = intersection[j] & set(self.buckets[bucket]) if len(intersection[j]) > 0 else set(self.buckets[bucket])
                    num_bucket_matches[j] += 1

        return intersection, num_bucket_matches

    # Function to finds all the other customers that are in the same bucket as the query
    def query_unions(self):

        unions = {j: set() for j in range(self.query_signatures.shape[1])}
        num_bucket_matches = {j: 0 for j in range(self.query_signatures.shape[1])}

        rows = self.query_signatures.shape[0] // self.band_size

        for i in range(rows):
            for j in range(self.query_signatures.shape[1]):
                bucket = (i, *self.query_signatures[i*self.band_size:(i+1)*self.band_size, j])
                if bucket in self.buckets.keys():
                    unions[j] = unions[j].union(self.buckets[bucket])
                    num_bucket_matches[j] += 1

        return unions, num_bucket_matches

    # Function to find the number of buckets that match for each query user
    def query_num_matching_buckets(self):
        
            num_matching_buckets = {j: dict() for j in range(self.query_signatures.shape[1])}
            rows = self.query_signatures.shape[0] // self.band_size

            for i, j in itertools.product(range(rows), range(self.query_signatures.shape[1])):

                bucket = (i, *self.query_signatures[i*self.band_size:(i+1)*self.band_size, j])
                if bucket not in self.buckets.keys():
                    continue
                    
                for cid in self.buckets[bucket]:
                    if cid not in num_matching_buckets[j].keys():
                        num_matching_buckets[j][cid] = 0
                    num_matching_buckets[j][cid] += 1
        
            return num_matching_buckets