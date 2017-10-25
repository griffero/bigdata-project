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
                yield data["business_id"], ["review_c", data["review_id"], data["stars"] ]
            else:
                yield data["business_id"], ["review", data["review_id"], data["stars"] ]
        elif data.get("business_id"):
            # business_id, ["business", categories]
            yield data["business_id"], ["categories", data["categories"]]

    def reducer_join(self, business_id, values):
        business_list = list(values)
        business_info = []
        have_coupon = False
        for business in business_list:
            if business[INPUT_LABEL] == "categories":
                business_info.append(business[BUSINESS_CATEGORIES])
            else:
                business_info.append(["s",business[REVIEW_STARS]])
                if business[INPUT_LABEL] == "review_c":
                    have_coupon = True
        if len(business_info) > 1:
            if have_coupon == True:
                business_info.insert(1, "r")
            else:
                business_info.insert(1, "rc")
            yield business_id, business_info

    def map_by_categories(self, key, business_info):
        categories = business_info[0]
        for element in business_info:
            if len(element) > 0:
                if element[INPUT_LABEL] == "s":
                    for category in categories:
                        yield category, [business_info[1] , element[BUSINESS_STARS]]

    def category_reducer(self, category, values):
        category_reviews = list(values)
        number_of_reviews_with_coupons = 0
        number_of_reviews_without_coupons = 0
        sum_of_stars_in_reviews_with_coupons = 0
        sum_of_stars_in_review_without_coupons = 0
        average_stars_of_coupons_reviews = "na"
        average_stars_of_coupons_without_reviews = "na"
        for review in category_reviews:
            if review[0] == "r":
                number_of_reviews_without_coupons +=1
                sum_of_stars_in_review_without_coupons += review[1]
            elif review[0] == "rc":
                number_of_reviews_with_coupons +=1
                sum_of_stars_in_reviews_with_coupons += review[1]
        if number_of_reviews_with_coupons > 0:
            average_stars_of_coupons_reviews = sum_of_stars_in_reviews_with_coupons/float(number_of_reviews_with_coupons)
        if number_of_reviews_without_coupons > 0:
            average_stars_of_coupons_without_reviews = sum_of_stars_in_review_without_coupons/float(number_of_reviews_without_coupons)
        if average_stars_of_coupons_reviews != "na" and average_stars_of_coupons_without_reviews != "na":
            #yield category, ["diference", average_stars_of_coupons_without_reviews - average_stars_of_coupons_reviews, number_of_reviews_with_coupons, number_of_reviews_without_coupons]
            yield category, ["diference", average_stars_of_coupons_without_reviews - average_stars_of_coupons_reviews]

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
