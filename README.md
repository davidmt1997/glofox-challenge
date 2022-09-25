## Data Engineer Challenge

This project was built using IntelliJ IDE and PySPark.

Before running the code install the libraries in the `requirements.txt` file.

### Source Code
The Spark jobs are in the **src** folder. They are sepparated by functionality.

1. **build_word_dictionary.py**: spark job to read all the words in the database and assigns an id to each one of them.
   This generates a sequence file in the output folder called: `word_dictionary`

2. **build_inverted_index.py**: spark job to build the inverted index matching all word ids with the files they appear in.
   This generates a text file in the output folder called: `inverted_index`
   
**Note** 
- The build_inverted_index job depends on the build_word_dictionary since it reads from its output. So, if that job hasn't run successfully it will exit the program.
- If the output is already generated, change the output folder to write in a new path. Otherwise the job will fail.

### Testing
In addition, testing functionality is implemented in the files within the `test` folder:

1. **test_word_dictionary.py**: 
   - Tests that the build_word_dictionary job produces a (word, id) RDD with unique elements.
2. **test_inverted_index.py**: 
   - Checks that the build_inverted_index job produces an RDD with unique elements.
   - Tests that the number of words is the same in both files produced.
   - Finally, it checks that the sampled words from the inverted index appear in the documents contained in the list of the inverted index and that they don't appear in the rest of the documents.
