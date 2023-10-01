import re
import json
import pickle
import sqlite3
import logging
logging.getLogger().setLevel(logging.INFO)

ENTITIES = ['&nbsp;', '&lt;', '&gt;', '&amp;', '&quot;',	
'&apos;', '&cent;', '&pound;', '&yen;' '&euro;', '&copy;', '&reg;']

IGNORE_SECTION = ['reference', 'references', 'see also', 'completed',
                  'track listing', 'gallery', '<gallery>']

IGNORE_THE_EXTRA_CONTENT = ['[[File:', '[[Category:', '[[Image:', '[[Category:']

#Checks the object if it's picklable or not. (:_:DEBUGGING PURPOSE:_:)
def is_picklable(obj):
    try:
        pickle.dumps(obj)
        return True
    except (pickle.PicklingError, TypeError):
        return False
    
def table_exists(cursor, table_name):
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cursor.fetchone() is not None    
    
def update_sqlite_table_with_dict(database_file, table_name, data_dict):

    try:

        # Connect to the SQLite database
        conn = sqlite3.connect(database_file)
        cursor = conn.cursor()

        # Check if the table exists
        if not table_exists(cursor, table_name):
            # If the table doesn't exist, create it
            create_table_sql = f"CREATE TABLE {table_name} (key TEXT PRIMARY KEY, value TEXT);"
            cursor.execute(create_table_sql)
        
        # Iterate through the dictionary and update the database
        for key, value_list in data_dict.items():
            # Serialize the list to JSON before inserting
            value_json = json.dumps(value_list)

            # Define the SQL update statement
            update_sql = f"INSERT OR REPLACE INTO {table_name} (key, value) VALUES (?, ?)"
            
            # Execute the SQL statement with the current key and serialized value
            cursor.execute(update_sql, (key, value_json))
        # Commit the changes to the database
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()
    
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"Error: {e}")    
    
def retrieve_data_from_sqlite(database_file, table_name):
    data_dict = {}
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(database_file)
        cursor = conn.cursor()

        # Retrieve data from the database
        cursor.execute(f"SELECT key, value FROM {table_name}")
        for key, value_json in cursor.fetchall():
            # Deserialize the JSON back to a list
            value_list = json.loads(value_json)
            data_dict[key] = value_list
        
        print("Data retrieved successfully.")
    
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the cursor and connection in the finally block
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return data_dict    

def get_first_sentence(text):
    #This function is for getting the first sentence of the text.
    #In this way, we can create a small size index. (Ideal)
    #The column will be used during the faiss-index creation.

    """
    :params text (dict): Main dict, must be used after self.cleaning_text()
    """


    fs_regex = re.compile(r'\.\s[A-Z].+', flags=re.MULTILINE)
    fixed_text = re.sub(fs_regex, '', text) + '.'
    fs_check = re.compile(r'\.\s+,\s*[a-z]')
    checked = re.search(fs_check, fixed_text)
    if checked:
        part = checked.group(0)
        replacement = '. ' + part[-1].upper()
        fixed_text = fixed_text.replace(part, replacement)
    return fixed_text    

def remove_nested_curly(text):
            stringus = ''
            bumble = 0
            for i in text:
                if i == '{':
                    bumble += 1
                elif i == '}'and bumble > 0:
                    bumble -= 1
                elif bumble == 0:
                    stringus += i
            return stringus

def cleaning_text(list_of_dicts, first_sentence=False):

    try:

        plain_text = {}

        """
        :params chunk (bool): If True, randomly extracts pages by chunk size. For debugging purposes. (default: False)
        """
        titles, ids, texts = ([] for x in range(3))

        titles = [key for d in list_of_dicts for key in d.keys()]
        ids = [value[0] for d in list_of_dicts for value in d.values()]
        texts = [value[1] for d in list_of_dicts for value in d.values()]


        for title, id, text in zip(titles, ids, texts):
            #We're gonna remove the HTML-like tags.
            text = re.sub("<!-+(.*?)-+>", '', text, flags=re.DOTALL)
            #Remove the part after 'IGNORE_SECTION'.
            _id = [text.find(part) for part in re.findall(r'=+(.*?)=+', text) if part.lower().strip() in IGNORE_SECTION]
            text = text[:min(_id)] if len(_id) != 0 else text

            # ==CURLY BRACKET REMOVER== # 
            text = remove_nested_curly(text)
            
            #Remove HTML-like tag pairs.
            text = re.sub("<(.*?)>(.*?)<\/(.*?)>", '', text, flags=re.MULTILINE)
            text = re.sub(r'<(.*?)>', '', text)
            
            #Checks the page and removes extra contents such as files and categories.
            for extra in IGNORE_THE_EXTRA_CONTENT:
                _ = 0
                _start = []
                _end = []
                while _ < len(text):
                    index = text.find(extra, _)
                    if index == -1:
                        break
                    _start.append(index)
                    _ = index + 1
                for idx in _start:
                    idx += 2
                    open_bracket_count = 2
                    for _letter in text[idx:]:
                        if open_bracket_count != 0:
                            if _letter == ']':
                                open_bracket_count -= 1
                            elif _letter == '[':
                                open_bracket_count += 1
                            else:
                                pass
                            idx += 1
                        else:
                            break
                    _end.append(idx)
                #Reversed lists to not cause errors during indexing.    
                reversed_start = _start[::-1]
                reversed_end = _end[::-1]
                for start, end in zip(reversed_start, reversed_end):
                    sub = text[start:end]
                    text = text.replace(sub, '')
            
            #We're accessing the data between the double brackets and check if it's useful for us or not. (Alias Remover)
            for part in re.findall(r'\[\[.+?\]\]', text, flags=re.DOTALL):
                if '|' in part:
                    second = part.split('|')[1].strip(']')
                    text = text.replace(part, second)
            
            #Then we're gonna remove all the links in the text.
            text = re.sub(r'https?:\/\/.*[\r\n]*', '', text, flags=re.MULTILINE)
            
            #Remove headlines, we don't need them.
            text = re.sub(r'=+(.*?)=+', '', text)
            #Remove the special characters, not cool.    
            for entity in ENTITIES:
                special = re.compile(entity)
                text = re.sub(special, '', text)
            #Remove the symbols we don't need.
            symbols = ["'", "{", "}", "*", '#', '"', "[", "]"] #() EKLE
            """
            for symbol in symbols:
                text = text.replace(symbol, '')
            """
            text = text.translate({ord(ch):'' for ch in symbols}) #NEW
            text = text.replace('()', '') #NEW
            #Some additional regex for wikipedia format.
            text = re.sub(r'\n\|.*\n', "", text)
            text = re.sub(r'\|+(.*?)\|+', "", text)
            three = re.escape('align=')+r'.*\n'
            text = re.sub(three, "", text)
            
            #New line and space remover, the data must be plain text.
            text = re.sub(r'\n+', ' ', text)
            text = re.sub(r'\s\s+', ' ', text)
            #Removing the meaningless tags.
            text = text.replace('&ndash;', '-')
            
            #Removing the trailing spaces, if there is any.
            text = text.lstrip(' ').rstrip(' ')
            title = title

            if first_sentence:

                text = get_first_sentence(text)

            plain_text.update({title:[id,text]})
   
        return plain_text    
    except Exception as e:
        print(e) 