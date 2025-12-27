import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: count_words_in_line <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("count-words-in-line")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("s3a://" + sys.argv[1])

    best = (text_file
            .map(lambda line: (len([w for w in line.split() if w]), line))
            .reduce(lambda a, b: a if a[0] >= b[0] else b))

    print("--------------------------------------------")
    print("Max number of words in a line:")
    print(best[0])
    print("Line content:")
    print(best[1])
    print("--------------------------------------------")
