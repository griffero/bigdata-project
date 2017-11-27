from __future__ import division
import operator
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
from mr3px.csvprotocol import CsvProtocol
import time
import tools
import csv

INPUT_LABEL = 0
BUSINESS_CATEGORIES = 2
BUSINESS_NAME = 1
BUSINESS_STARS = 1
REVIEW_ID = 1
REVIEW_STARS = 2
LIST_SIZE = 5
REVIEW_COUPON = 3

class ImpactPerBuiness(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def separate_map_by_stars_and_categories(self, _, line):
        data = line
        if data.get("review_id"):
            # business_id, ["review", user_id, stars]
            has_coupon = False
            COUPONS = tools.read_coupons_file()
            for coupon in COUPONS:
                if coupon in data["text"].lower():
                    has_coupon = True
                    yield data["business_id"], ["review_c", data["review_id"], data["stars"], coupon]
            if has_coupon == False:
                yield data["business_id"], ["review", data["review_id"], data["stars"]]
        elif data.get("business_id"):
            # business_id, ["business_info", name, categories]
            yield data["business_id"], ["business_info", data["name"], data["categories"]]

    def reducer_join(self, business_id, values):
        business_list = list(values)
        business_categories = []
        business_name = ""
        stars_with_coupon = []
        stars_without_coupon = []
        coupon_list = []
        for business in business_list:
            if business[INPUT_LABEL] == "business_info":
                business_categories = business[BUSINESS_CATEGORIES]
                business_name = business[BUSINESS_NAME]
            else:
                if business[INPUT_LABEL] == "review_c":
                    stars_with_coupon.append(business[REVIEW_STARS])
                    coupon_list.append(business[REVIEW_COUPON])
                elif business[INPUT_LABEL] == "review":
                    stars_without_coupon.append(business[REVIEW_STARS])
        if len(business_categories) > 0 and len(stars_with_coupon) > 0 and len(stars_without_coupon):
            for coupon in coupon_list:
                for category in business_categories:
                    coupon_output = [business_name.encode('utf-8'), coupon, reduce(lambda x, y: x + y, stars_with_coupon) / len(stars_with_coupon), category]
                    writer.writerow(coupon_output)
                    yield business_name, coupon_output
            for category in business_categories:
                no_coupon_output = [business_name.encode('utf-8'), "No coupon", reduce(lambda x, y: x + y, stars_without_coupon) / len(stars_without_coupon), category]
                writer.writerow(no_coupon_output)
                yield business_name, no_coupon_output


    def steps(self):
        return [
            MRStep(
                mapper=self.separate_map_by_stars_and_categories,
                reducer=self.reducer_join
                )
            ]

if __name__ == '__main__':
    start_time = time.time()
    header = ["name", "has_coupon", "average", "categories"]
    output_file  = open('data10.csv', "wb")
    writer = csv.writer(output_file)
    writer.writerow(header)
    ImpactPerBuiness.run()
    print 'Time lapsed: {} seconds.'.format(time.time() - start_time)
