# A sample contract to test if the coverage metrics
# given by coverage.py are correct.
display_name = "Empty Product"
api = "3.2.0"
version = "0.1.0"
tside = Tside.LIABILITY
supported_denominations = ["GBP"]
parameters = []


def pre_posting_code(postings, effective_date):
    if postings:
        x = 1
        y = 2
        z = 3
        return x + y + z
    else:
        return
