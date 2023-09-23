import pandas as pd
import xml.dom.minidom
import re
import concurrent.futures
from multiprocessing import get_context, cpu_count, Process, Pool
import logging
logging.getLogger().setLevel(logging.INFO)
from time import perf_counter
import os
from random import sample

import string

#Path to the file.
input_file = 'enwiki-20230901-pages-articles-multistream11.xml-p6899367p7054859'

elements = xml.dom.minidom.parse(input_file).documentElement.getElementsByTagName('page')

dir_path = os.path.dirname(os.path.realpath(__file__))
default_cpu_count = cpu_count()
IGNORE_THE_EXTRA_CONTENT = ['[[File:', '[[Category:', '[[Image:']
IGNORE_SECTION = ['reference', 'references',
                      'see also',
                      'completed',
                      'track listing',
                      'gallery']
IGNORE_REDIRECTS = ['#REDIRECT', '#redirect', '#Redirect', 'may refer to']
ENTITIES = ['&nbsp;', '&lt;', '&gt;', '&amp;', '&quot;',	
'&apos;', '&cent;', '&pound;', '&yen;' '&euro;', '&copy;', '&reg;']
#OUTPUT FILES
output_files = list(string.ascii_lowercase) + [str(number) for number in range(0,10)]

def split_page(chunk=False):
    #Splits the entire page into smaller pages and updates the self.extraction (dict).
    extraction = {}
    """
    :params chunk (bool): For debugging purpose. False (default).
    """
    #Empty the dict in case there is any data.
    CHUNK_SIZE = 50
    page_count = 0
    if chunk:
        _ = sample(elements, CHUNK_SIZE)
    else:
        _ = elements
    for element in _:
        title = element.getElementsByTagName('title')[0].childNodes[0].nodeValue
        text = element.getElementsByTagName('text')[0].childNodes[0].nodeValue
        ns = element.getElementsByTagName('ns')[0].childNodes[0].nodeValue
        id = element.getElementsByTagName('id')[0].childNodes[0].nodeValue
        if any(redirect in text for redirect in IGNORE_REDIRECTS):
            continue
        else:
            #Get the text, nothing else.
            if ns == '0':
                extraction.update({id:[title, text]})
                page_count += 1
    logging.info(f'Split {page_count} pages!')
    return extraction

def cleaning_text(chunk):

    beginning = perf_counter()

    plain_text = {}

    """
    :params chunk (bool): If True, randomly extracts pages by chunk size. For debugging purposes. (default: False)
    """

    extraction = split_page(chunk)
    ids, titles, texts = ([] for x in range(3))
   
    ids = list(extraction.keys())[:]
    titles = [d[0] for d in extraction.values()][:]
    texts = [d[1] for d in extraction.values()][:]

    for id, title, text in zip(ids, titles, texts):
        #We're gonna remove the HTML-like tags.
        text = re.sub("<!-+(.*?)-+>", '', text, flags=re.DOTALL)
        #Remove the part after 'IGNORE_SECTION'.
        _id = [text.find(part) for part in re.findall(r'=+(.*?)=+', text) if part.lower().strip() in IGNORE_SECTION]
        text = text[:min(_id)] if len(_id) != 0 else text

        # ==CURLY BRACKET REMOVER== # 
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
        text.translate({ord(ch):'' for ch in symbols}) #NEW
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
        plain_text.update({id:[title,text]})

    finish = perf_counter()    
    logging.info('Cleaning process took only %d seconds. Not fast, huh?', finish - beginning) 

    ###### SECOND PART ######

    root_path = 'bundle'
    output_path = os.path.join(dir_path, root_path)
    if root_path not in os.listdir(dir_path):
        os.makedirs(output_path)

    for letter in output_files:

        path_parquet = output_path + '/' + letter + '.parquet.gzip'
        print("PLAIN TEXT LENGTH: ", len(plain_text))
        _ids = list(plain_text.keys())[:]
        _titles = [d[0] for d in plain_text.values()]
        _texts = [d[1] for d in plain_text.values()]
        id_list = [y for x, y in zip(_titles, _ids) if x[0].lower() == letter]

        TEMP_DICT = {}
        
        for di, dt, de in zip(_ids, _titles, _texts):
            if di in id_list:
                TEMP_DICT.update({di: [dt, de]})
        TEMP_DF = pd.DataFrame.from_dict(TEMP_DICT, orient='index', columns=['title', 'text'])    
        
        TEMP_DF.to_parquet(path_parquet, compression='gzip')
        
        logging.info(f'{letter}/Dump was successful to {output_path}')

        plain_text = {id:[title, text] for id, [title, text] in plain_text.items() if id not in id_list}

def main():

    main_start = perf_counter()

    cleaning_text(chunk=False)

    main_end = perf_counter()

    print(f'Main Process took {main_end-main_start} seconds.')    

if __name__ == '__main__':

    main()     