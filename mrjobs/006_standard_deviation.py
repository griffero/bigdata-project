from __future__ import division

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import time
import tools
import math


class UniqueReview(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper_input(self, _, line):
        COUPONS = tools.read_coupons_file()
        if any(word in line['text'].lower() for word in COUPONS):
            yield 'AVERAGE', line['stars']

    def reducer_01(self, promo_name, value):
        stars = list(value)
        qty = len(stars)
        avg = sum(stars) / qty
        diff = [s - avg for s in stars]
        diff_sqrt = [d ** 2 for d in diff]
        yield 'DESV', math.sqrt(sum(diff_sqrt) / (qty - 1))

    def steps(self):
        return [MRStep(
            mapper=self.mapper_input,
            reducer=self.reducer_01
        )]


if __name__ == '__main__':
    start_time = time.time()
    UniqueReview.run()
    print 'Time lapsed: {} seconds.'.format(time.time() - start_time)
