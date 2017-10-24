import os


def read_coupons_file():
    REL_FOLDER_PATH = os.path.dirname(os.path.abspath(__file__))
    COUPON_FOLDER_PATH = os.path.join(REL_FOLDER_PATH, 'coupons.txt')
    coupons = []
    with open(COUPON_FOLDER_PATH) as f:
        for line in f:
            coupons.append(line.split('\n')[0])

    return coupons