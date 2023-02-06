from crawler.analyzers.analyzer import Analyzer
from crawler.models import Share


# from datetime import date, timedelta
# import matplotlib.pyplot as plt

# import pandas as pd


class GoodPriceRightIssueAnalyzer(Analyzer):
    def analyze(self, share):
        if share.is_rights_issue:
            main_share = share.base_share

            if main_share.last_day_history['close'] / (share.last_day_history['close'] + 1000) > 1.1:
                return {"good price right issue": {"price": share.last_day_history['close'],
                                                   "main price": main_share.last_day_history['close']}}
