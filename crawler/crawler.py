import random
import time
from datetime import datetime

def start_crawler ():
    while True:
        print(datetime.now())
        time.sleep(random.uniform(0.5,1.5))
        print("heloooo crawler")

