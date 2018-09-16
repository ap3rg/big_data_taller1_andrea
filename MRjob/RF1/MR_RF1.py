from mrjob.job import MRJob
from mrjob.step import MRStep


class MRMostUsedWord(MRJob):

    #supporting methods
    def mapper_init():
        '''
        This method generates a dictionary from the file taxi_zone_lookup.csv
        '''

        # Absolute path, script is in
        script_dir = os.path.dirname(__file__)
        # relative path
        rel_path = 'LocationByID/taxi_zone_lookup.csv'
        abs_file_path = os.path.join(script_dir, rel_path)

        # get data to format index to use in location by data method
        f = open(abs_file_path, 'r')
        data = f.read()
        data = data.split('\n')

        zones = []
        location_by_ID = {}

        for line in data:
            line = line.strip()
            # print(line)
            location, zone, trash = line.split(',', 2)

            if zone not in zones:
                location_by_ID[zone] = []

        for line in data:
            line = line.strip()
            location, zone, trash = line.split(',', 2)

            if (location not in location_by_ID[zone]):
                location_by_ID[zone].append(location)

        return location_by_ID

        # Create the location dictionary

    location_dictionary = location_by_ID()




    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

    def mapper_get_words(self, _, line):

        # input comes from STDIN (standard input)
        # Input enters in a CSV scheme
        # Each line is a record

        line = line.strip()
        data = line.split(',')

        destinationID = get_value('END_LOC', data)
        destination = location_by_ID_lookup(location_dictionary, destinationID)

        pickupTime = get_value('START_DATE', data)
        pickupTime = datetimeToInt(pickupTime)
        if destination:
            if inTimeRange(start, end, pickupTime):
                print('%s\t%s' % (destination, 1))

        # yield each word in the line
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)

    # discard the key; it is just None
    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield max(word_count_pairs)


if __name__ == '__main__':
    MRMostUsedWord.run()