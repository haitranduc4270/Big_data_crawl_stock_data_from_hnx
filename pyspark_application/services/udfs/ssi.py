from pyspark.sql.functions import udf
from pyspark.sql.types import *
import random


@udf(returnType=LongType())
def price_ssi(price):
    if price is None:
        return 0
    return price * 10


@udf(returnType=LongType())
def volume_ssi(volume):
    if volume is None:
        return 0
    return volume * 1
