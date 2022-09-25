from modules.utils import get_spark_context, clean_word, get_doc_id, process_file, DATASET_PATH, OUTPUT_PATH
from os import listdir
import sys


def check_word_dictionary_exists():
    try:
        listdir(OUTPUT_PATH)
    except FileNotFoundError as e:
        raise Exception("Word dictionary not found. Execute build_word_dictionary.py to create it")
        sys.exit()


# Reads all the files in the dataset in a (file_name, word) RDD
# Performs cleaning processes for the words
# Swaps the RDD pair in the format (clean_word, file_name)
# Groups all file_names by word to produce a new RDD (word, list(file_names))
# Reads the word dictionary from the previous process
# Builds a new RDD with the format (word_id, list(file_names)) by mapping words to their id
# Drops duplicated file_names in the list for each key
# Writes resulting RDD to a file
def get_inverted_index():

    check_word_dictionary_exists()

    sc = get_spark_context()
    output = sc.wholeTextFiles(DATASET_PATH).flatMapValues(
        lambda content: process_file(content)).distinct() \
        .map(lambda x: (clean_word(x[1]), [get_doc_id(x[0])])) \
        .filter(lambda x: x[0] != "" and not x[0].isdigit()) \
        .reduceByKey(lambda a, b: a + b)

    words = sc.sequenceFile(f'{OUTPUT_PATH}/word_dictionary')
    words_dict = words.collectAsMap()

    output_mapped = output.map(lambda x: (words_dict[x[0]], x[1]))
    output_mapped_unique = output_mapped.map(lambda x: (x[0], list(set(x[1]))))

    output_mapped_unique.saveAsTextFile(f'{OUTPUT_PATH}/inverted_index')


if __name__ == '__main__':

    get_inverted_index()
