from modules.utils import get_spark_context, clean_word, process_file, OUTPUT_PATH, DATASET_PATH
from os import listdir
from os.path import isfile, join


# test that there are not duplicates
def test_unique_words():
    inverted_index_count = inverted_index.count()
    inverted_index_count_unique = inverted_index.distinct().count()

    assert inverted_index_count == inverted_index_count_unique


# Test that the number of words in the word dictionary is the same as in the inverted index generated
def test_number_of_words_processed():
    word_dict_count = words.distinct().count()
    inverted_index_count = inverted_index.distinct().count()

    assert word_dict_count == inverted_index_count


# Returns the number of files in the directory passed as an argument
def get_number_of_files_in_dataset(folder):
    files = [f for f in listdir(folder) if isfile(join(folder, f))]
    return len(files)


# Checks if word appears in the file_id specified as arguments
def word_in_file(word, file_id):
    word_dict = sc.textFile(f'{DATASET_PATH}/{file_id}').flatMap(lambda line: process_file(line)) \
        .map(lambda w: clean_word(w)) \
        .filter(lambda x: x != "" and not x.isdigit()) \
        .distinct()

    return word_dict.filter(lambda a: a == word).count() > 0


# Utility function to convert list of strings to list of ints
# Used when reading the inverted index file
def convert_to_int(lst):
    ret = []
    for e in lst:
        ret.append(int(e.replace("'", "")))
    return ret


# Utility function to parse the inverted index file
def process_inverted_index(row):
    return row.replace(' ', '').replace('(', '').replace(')', '').replace('[', '').replace(']', '').split(",", 1)


# Test that random words from the inverted index doc actually appear in the documents of the inverted index
# And do NOT appear in the other documents
def test_inverted_index(fraction):
    # Since the inverted index file was saved as a textFile, we need to process the input and convert it to pairs
    # in the form (word_id, [list of documents])
    inverted_index_mapped = inverted_index.map(lambda row: process_inverted_index(row)) \
        .map(lambda x: (int(x[0]), convert_to_int(list(x[1].split(',')))))

    inverted_index_sampled = inverted_index_mapped.sample(False, fraction)

    words_swapped = words.map(lambda x: (x[1], x[0]))

    word_dict = words_swapped.collectAsMap()

    inverted_index_mapped = inverted_index_sampled.map(lambda x: (word_dict[x[0]], x[1]))
    inverted_index_count = inverted_index_mapped.count()
    print(f"Checking: {inverted_index_count} number of words in inverted index file")

    doc_id_list = list(range(get_number_of_files_in_dataset(DATASET_PATH)))

    for i in inverted_index_mapped.take(inverted_index_count):
        print(i)

        # Checking that the words appear in the documents of the inverted index
        for j in i[1]:
            assert word_in_file(i[0], j)

        # Checking that the same words don't appear in the rest of the documents
        docs_not_containing_word = [item for item in doc_id_list if item not in i[1]]
        for j in docs_not_containing_word:
            assert not word_in_file(i[0], j)


if __name__ == '__main__':
    sc = get_spark_context()

    # Persist both RDDs to be used in ech of the tests
    words = sc.sequenceFile(f'{OUTPUT_PATH}/word_dictionary').persist()
    inverted_index = sc.textFile(f'{OUTPUT_PATH}/inverted_index').persist()

    print("Starting tests for inverted index...")
    test_unique_words()
    print("Unique word test passed!")
    test_number_of_words_processed()
    print("Number of words test passed!")

    # Fraction used to sample the number of rows used from the inverted index
    my_fraction = 0.0001
    test_inverted_index(my_fraction)
    print("Inverted index test passed!")
