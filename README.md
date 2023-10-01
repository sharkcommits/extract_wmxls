# Wikipedia Extractor

Extractipedia aims to create plain text Wikipedia pages in order to create faiss-like indexes for machine learning projects.

<b>Input file:</b> Wikimedia XLS file which can be found on [Wikimedia Dumps](https://dumps.wikimedia.org/enwiki/).

<b>Output file:</b> SQLite database.

## Basic Usage:

```
python3 extractipedia.py [-f] file_name 
```
```
[-f, --file_name] ==> File Name(str): Name of the Wikipedia Dump File (.xml)
```
## Tuning the Script:

```
python3 extractipedia.py [-f] file_name [-b] batch_size [-d] database_file
[-t] table_name [-n] num_workers [-s] first_sentence
```

### Additional Arguments:

```
[-b, --batch_size] ==> Batch Size(int): RAM usage increases as the batch size gets bigger. (default = 2500)
[-d, --database_file] ==> Database File(str): Name of the SQLite database. The script will create for you if the file does not exist. (default = 'new_database.db')
[-t, --table_name] ==> Table Name(str): Name of the table for the database above. It will be created if it does not exist. (default = 'new_table')
[-n, --num_workers] ==> Number of Workers(int): Each process runs on different core. So the maximum process number equals to the cores that your machine has. But it is advisable that you should at least exclude 1 core in order to give your machine breathing room. You can give the core number directly. (default = -2)
[-s, --first_sentence] ==> First Sentence(bool): If you need just the first sentence of a page. Change it to True. It's faster and memory-friendly. (default = False)
```

### Getting Help:

```
[-h, --help] ==> It will print out the necessary information.
```

## Potential Improvements:

- Increase speed, using multiprocessing. (Done!)
- Simplify the regex, more human-readable.
- Get the templates and split the entire .xls into seperate and desired files.
- Get the tables (if there is any) and process them.

### Let there be extraction!