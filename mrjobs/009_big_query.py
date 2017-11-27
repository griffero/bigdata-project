from __future__ import division
import operator

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import time
import tools

INPUT_LABEL = 0
BUSINESS_CATEGORIES = 2
BUSINESS_NAME = 1
BUSINESS_STARS = 1
REVIEW_ID = 1
REVIEW_STARS = 2
LIST_SIZE = 5

class ImpactPerBuiness(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def separate_map_by_stars_and_categories(self, _, line):
        data = line
        if data.get("review_id"):
            # business_id, ["review", user_id, stars]
            COUPONS = tools.read_coupons_file()
            if any(word in data['text'].lower() for word in COUPONS):
                yield data["business_id"], ["review_c", data["review_id"], data["stars"] ]
            else:
                yield data["business_id"], ["review", data["review_id"], data["stars"] ]
        elif data.get("business_id"):
            # business_id, ["business_info", name, categories]
            yield data["business_id"], ["business_info", data["name"], data["categories"]]

    def reducer_join(self, business_id, values):
        business_list = list(values)
        business_categories = []
        business_name = ""
        stars_with_coupon = []
        stars_without_coupon = []
        for business in business_list:
            if business[INPUT_LABEL] == "business_info":
                business_categories = business[BUSINESS_CATEGORIES]
                business_name = business[BUSINESS_NAME]
            else:
                if business[INPUT_LABEL] == "review_c":
                    stars_with_coupon.append(business[REVIEW_STARS])
                elif business[INPUT_LABEL] == "review":
                    stars_without_coupon.append(business[REVIEW_STARS])
        if len(business_categories) > 0 and len(stars_with_coupon) > 0 and len(stars_without_coupon):
            yield business_name, ["with coupon", reduce(lambda x, y: x + y, stars_with_coupon) / len(stars_with_coupon), business_categories]
            yield business_name, ["without coupon", reduce(lambda x, y: x + y, stars_without_coupon) / len(stars_without_coupon), business_categories]

    def steps(self):
        return [
            MRStep(
                mapper=self.separate_map_by_stars_and_categories,
                reducer=self.reducer_join
                )
            ]

if __name__ == '__main__':
    start_time = time.time()
    ImpactPerBuiness.run()
    print 'Time lapsed: {} seconds.'.format(time.time() - start_time)
