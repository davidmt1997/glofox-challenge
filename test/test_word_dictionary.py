from modules.utils import get_spark_context, OUTPUT_PATH


# Test that there are not duplicated words in the dictionary generated
def test_unique_words():
    sc = get_spark_context()
    word_dict = sc.sequenceFile(f'{OUTPUT_PATH}/word_dictionary')
    word_dict_count = word_dict.count()
    word_dict_count_unique = word_dict.distinct().count()

    word_dict_map = word_dict.collectAsMap()
    assert len(word_dict_map) == len(set(word_dict_map.values()))
    assert word_dict_count == word_dict_count_unique


if __name__ == '__main__':
    print("Starting test for word dictionary")
    test_unique_words()
    print("Word dictionary test passed!")
