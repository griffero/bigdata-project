from __future__ import division

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

class UniqueReview(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def separate_map_by_stars_and_categories(self, _, line):
        data = line
        if data.get("review_id"):
            # business_id, ["review", user_id, stars]
            COUPONS = tools.read_coupons_file()
            if any(word in data['text'].lower() for word in COUPONS):
                yield data["business_id"], ["review", data["review_id"], data["stars"] ]
        elif data.get("business_id"):
            # business_id, ["business", categories]
            yield data["business_id"], ["categories", data["categories"]]

    def reducer_join(self, business_id, values):
        business_list = list(values)
        business_info = []
        for business in business_list:
            if business[INPUT_LABEL] == "categories":
                business_info.append(business[BUSINESS_CATEGORIES])
            else:
                business_info.append(business)
        if len(business_info) > 1:
            yield business_id, business_info

    def map_by_categories(self, key, business_info):
        categories = business_info[0]
        for element in business_info:
            if element[INPUT_LABEL] == "review":
                for category in categories:
                    yield category, element[REVIEW_STARS]

    def category_reducer(self, category, stars):
        stars_list = list(stars)
        number_of_reviews = len(stars_list)
        average_stars = sum(stars_list)/float(number_of_reviews)
        yield category, average_stars

    def steps(self):
        return [
            MRStep(
                mapper=self.separate_map_by_stars_and_categories,
                reducer=self.reducer_join
                ),
            MRStep(
                mapper=self.map_by_categories,
                reducer=self.category_reducer
                )
            ]


if __name__ == '__main__':
    start_time = time.time()
    UniqueReview.run()
    print 'Time lapsed: {} seconds.'.format(time.time() - start_time)
