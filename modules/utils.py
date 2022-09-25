# Utility functions and variables used in more than one file

import unidecode
import string
from pyspark.sql import SparkSession


DATASET_PATH = '../dataset/'
OUTPUT_PATH = '../output/'


def get_spark_context():
    spark = SparkSession \
        .builder \
        .master('local[1]') \
        .appName("Python Spark Glofox challenge") \
        .getOrCreate()

    return spark.sparkContext


# Utility function to clean the words read
def clean_word(word):
    # remove accents
    word = unidecode.unidecode(word)
    # remove digits in word
    result = ''.join([i for i in word if not i.isdigit()])
    # replace new lines, tabs, etc
    result = result.replace('\n', '').replace('\t', '')
    # remove punctuation characters like ! ? # ...
    return result.translate(str.maketrans('', '', string.punctuation))


# Utility function to clean the files read
def process_file(content):
    return content.lower().replace('\n', ' ').replace('\t', ' ').replace('\r', ' ').split(" ")


# Returns the file_id of a given path
def get_doc_id(file_path):
    folders = file_path.split('/')
    return folders[len(folders) - 1]
