import sys
from pyspark import SparkContext, SparkConf

def clean_word(word):
    while word.endswith(".") or word.endswith(","):
        word = word[:-1]
    return word

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: longestword <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("python-longest-word")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("s3a://" + sys.argv[1])

    words = (text_file
             .flatMap(lambda line: line.split(" "))
             .map(clean_word)
             .filter(lambda w: len(w) > 0 and w.isalpha()))

    longest = words.reduce(lambda a, b: a if len(a) >= len(b) else b)

    print("--------------------------------------------")
    print(longest)
    print(len(longest))
    print("--------------------------------------------")
