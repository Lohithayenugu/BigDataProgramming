from builtins import print, list, len, set
from pyspark import SparkContext


def map_friends(line):
    people = line.split(" ")
    person = people[0]
    friend = line[1]
    return person, friend


def map_togroup(tuple):
    person = tuple[0]
    final = []
    for friend in tuple[1]:
        if person < friend:
            key = person + "," + friend
        else:
            key = friend + "," + person
        value = key, list(tuple[1])
        final.append(value)
    return final


def reduce(key, value):
    return list(set(key) & set(value))


def run(input, output):
    sc = SparkContext.getOrCreate()
    lines = sc.textFile(input, 1)
    mapped_friends = lines.map(map_friends).groupByKey()
    grouped_friends = mapped_friends.flatMap(map_togroup)
    mutual_friends = grouped_friends.reduceByKey(reduce).filter(lambda x: len(x[1]) > 0)
    print(mutual_friends.collect())
    mutual_friends.coalesce(1).saveAsTextFile(output)


if __name__ == "__main__":
    run("facebook_combined.txt", "facebook_mutual_friends")
