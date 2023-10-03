# Extractipedia

Extractipedia aims to create plain text Wikipedia pages in order to use indexing while training machine learning models.

<b>Input file:</b> Wikimedia XLS file which can be found on [Wikimedia Dumps](https://dumps.wikimedia.org/enwiki/).

<b>Output file:</b> SQLite database file.

## Basic Usage:

```
python -m extractipedia.Extraction -f file_name.xml 
```
```
[-f, --file_name] ==> File Name(str): Name of the Wikipedia Dump File (.xml)
```
## Tuning into the Script (Advanced):

```
python -m extractipedia.Extraction -f file_name.xml -b batch_size -d database_file.db
-t table_name -n num_workers -s [first_sentence]
```

```
[-b, --batch_size] ==> Batch Size(int): RAM usage increases as the batch size gets bigger. (default = 2500)
[-d, --database_file] ==> Database File(str): Name of the SQLite database. The script will create for you if the file does not exist. (default = 'new_database.db')
[-t, --table_name] ==> Table Name(str): Name of the table for the database above. It will be created if it does not exist. (default = 'new_table')
[-n, --num_workers] ==> Number of Workers(int): Each process runs on different core. So the maximum process number equals to the cores that your machine has. But it is advisable that you should at least exclude 1 core in order to give your machine breathing room. You can give the core number directly. (default = max - 2)
[-s, --first_sentence] ==> First Sentence(bool): If you need just the first sentence of a page, just use -s flag. It's memory-friendly. (default = False)
```

## Check out your database once it is created:

You can check out your database with the command below.

```
python -m extractipedia.CheckDatabase -f YOUR_DATABASE.db -t YOUR_TABLE -c chunk_size -r [random]
```

```
(optional) [-c, --chunk_size] ==> Chunk Size(int): It will retrieve the first n items from your database and you can check out if there is any errors. (default = 10)
(optional) [-r, --random] ==> Random(bool): If you want to retrieve random n items, just use -r flag. (default = False)
```

### Getting Help:

```
[-h, --help] ==> It will print out the necessary information.
```

## Potential Improvements:

- Increase speed, using multiprocessing. (Done!)
- Progress bar. (Coming soon!)
- Simplify the regex, more human-readable.
- Get the templates and split the entire .xls into seperate and desired files.
- Get the tables (if there is any) and process them.

### Let there be extraction!