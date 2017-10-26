from __future__ import division
import operator

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import time
import tools

INPUT_LABEL = 0
BUSINESS_CATEGORIES = 1
BUSINESS_STARS = 1
REVIEW_ID = 1
REVIEW_STARS = 2
LIST_SIZE = 5

class UniqueReview(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def map_by_presence_of_coupons(self, _, line):
        data = line
        COUPONS = tools.read_coupons_file()
        if any(word in data['text'].lower() for word in COUPONS):
            yield "review_c", data["stars"]
        else:
            yield "review", data["stars"]

    def average_reducer(self, key, values):
        stars = list(values)
        average_stars = sum(stars)/float(len(stars))
        if key == "review":
            yield "Promedio sin cupon: ", average_stars
        elif key == "review_c":
            yield "Promedio con cupon: ", average_stars

    def steps(self):
        return [
            MRStep(
                mapper=self.map_by_presence_of_coupons,
                reducer=self.average_reducer
                ),
            ]

if __name__ == '__main__':
    start_time = time.time()
    UniqueReview.run()
    print 'Time lapsed: {} seconds.'.format(time.time() - start_time)
