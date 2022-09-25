from modules.utils import get_spark_context, clean_word, process_file, DATASET_PATH, OUTPUT_PATH


# Reads all the files in the dataset folder and splits the files by words
# Performs cleaning processes for each of the words
# Assigns an id to each word to generate an RDD in the form (word, word_id)
# Sorts the RDD by word
# Writes the resulting RDD into a file
def get_word_dictionary():
    sc = get_spark_context()
    words = sc.textFile(DATASET_PATH).flatMap(lambda line: process_file(line)) \
        .map(lambda word: clean_word(word)) \
        .filter(lambda x: x != "" and not x.isdigit()) \
        .distinct().zipWithIndex()

    words_sorted = words.sortBy(lambda x: x[0])
    words_sorted.saveAsSequenceFile(f'{OUTPUT_PATH}/word_dictionary')


if __name__ == '__main__':
    get_word_dictionary()
