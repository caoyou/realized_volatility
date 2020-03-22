import json
df = sc.textFile("file:////data/btc.txt")
def handle(l):
    try:
        time, js = l.split("==recv==")
        data = json.loads(js)
        return (data["data"],)
    except:
        print("ERROR LINE: {}".format(l))
        return (None, )

def process_timestamp(t):
    return t.replace('T', ' ').replace('Z', '').split(".")[0]

df = df.map(handle).toDF().filter("_1 is not null").selectExpr("explode(_1)")\
       .rdd.map(lambda x: (x.col['side'], float(x.col['size']), float(x.col['price']), process_timestamp(x.col['timestamp'])))\
       .toDF(["side", "size", "price", "timestamp"])
import pyspark.sql.functions as F
from pyspark.sql.types import *

def first(l):
    return l[0]
def last(l):
    return l[-1]    

def volume(sides, sizes, direction):
    return sum([sizes[i] for i, v in enumerate(sides) if v == direction])

first = F.udf(first, DoubleType())
last = F.udf(last, DoubleType())
from functools import partial
bvolume = F.udf(partial(volume, direction='buy'), DoubleType())
svolume = F.udf(partial(volume, direction='sell'), DoubleType())


klines = df.groupby(F.window("timestamp", "1 minutes")).agg(F.max("price").alias("max"), 
                                                F.min("price").alias("min"),
                                                first(F.collect_list("price")).alias("open"),
                                                last(F.collect_list("price")).alias("close"),
                                                F.sum("size").alias("volume"),
                                                bvolume(F.collect_list("side"), F.collect_list("size")).alias("buy_volume"),
                                                svolume(F.collect_list("side"), F.collect_list("size")).alias("sell_volume")
                                                )
klines = klines.select(
    klines.window.end.cast("string").alias("time"), 
    "max", "min", "open", "close", "buy_volume", 
    "sell_volume", "volume").sort("time")