import pandas as pd
import xml.dom.minidom
import re
from multiprocessing import Queue, get_context, cpu_count
import logging
from time import perf_counter
import sys
import os
from io import StringIO
from random import sample
import csv
import json

#Path to the file.
input_file = 'enwiki-20230901-pages-articles-multistream11.xml-p6899367p7054859'

class WikiText():

    #Constructing a class which handles everything.
    def __init__(self, file):

        self.file = file
        self.elements = xml.dom.minidom.parse(self.file).documentElement.getElementsByTagName('page')
        
        #self.group = self.domtree.documentElement
        #self.elements = self.group.getElementsByTagName('page')

        self.dir_path = os.path.dirname(os.path.realpath(__file__))

        #self.titles = []
        #self.texts = []
        #self.ids = []
        self.plain_text = {}

        self.extraction = {}

        self.length = len(self.elements)

        self.LANGUAGE_CODES = ['en', 'de', 'fr', 'ja', 'es', 'ru', 'pt', 'zh', 'it', 'fa',
                  'pl', 'ar', 'nl', 'uk', 'he', 'id', 'tr', 'sv', 'cs', 'vi',
                  'ko', 'fi', 'hu', 'simple', 'hi', 'th', 'no', 'ca', 'el', 'bn',
                  'ro', 'da', 'sr', 'uz', 'bg', 'ms', 'az', 'et', 'sk', 'hr',
                  'hy', 'lt', 'eo', 'sl', 'ur', 'eu', 'zh-yue', 'lv', 'be', 'ml',
                  'ta', 'gl', 'mk', 'sq', 'kk', 'ha', 'ka', 'te', 'arz', 'mr',
                  'af', 'sh', 'ceb', 'sw', 'bs', 'la', 'kn', 'tl', 'be-tarask',
                  'my', 'nn', 'ckb', 'is', 'azb', 'jv', 'pa', 'oc', 'ku', 'as',
                  'mn', 'cy', 'so', 'ig', 'ast', 'tt', 'zh-min-nan', 'br', 'ne',
                  'sco', 'lmo', 'ga', 'yo', 'ky', 'pms', 'lb', 'als', 'war', 'ba',
                  'si', 'fy', 'km', 'vec', 'bcl', 'an', 'gu', 'or', 'zh-classical',
                  'gpe', 'bar', 'tg', 'am', 'pnb', 'bjn', 'ps', 'io', 'tw', 'cv',
                  'yi', 'bh', 'scn', 'qu', 'su', 'fo', 'mt', 'ce', 'eml', 'mg', 'nap',
                  'wuu', 'ht', 'min', 'ary', 'nds', 'ace', 'sc', 'sa', 'sat', 'lo',
                  'lld', 'szl', 'zu', 'ia', 'li', 'mzn', 'tk', 'ban', 'mai', 'rw',
                  'lij', 'sah', 'ang', 'diq', 'vo', 'dag', 'tly', 'crh', 'gn', 'sd',
                  'hyw', 'kaa', 'avk', 'ext', 'tcy', 'co', 'hsb', 'bat-smg', 'hif',
                  'vep', 'frp', 'ff', 'kab', 'pam', 'dv', 'om', 'pap', 'bo', 'vls',
                  'rm', 'nds-nl', 'ab', 'xmf', 'rue', 'wa', 'os', 'gd', 'cbk-zam',
                  'hak', 'haw', 'ln', 'mad', 'mi', 'roa-tara', 'ie', 'dsb', 'guc',
                  'zea', 'ay', 'lfn', 'frr', 'tpi', 'ug', 'xh', 'bpy', 'kw', 'mwl',
                  'new', 'se', 'blk', 'pcd', 'shi', 'av', 'bi', 'ilo', 'csb', 'cu',
                  'lad', 'fur', 'gan', 'lez', 'gv', 'nah', 'udm', 'dty', 'dz', 'smn',
                  'kv', 'mhr', 'skr', 'tet', 'fiu-vro', 'ks', 'ami', 'atj', 'chr',
                  'cdo', 'mrj', 'mdf', 'nv', 'nov', 'ady', 'awa', 'bug', 'gag', 'gor',
                  'gur', 'olo', 'st', 'ss', 'tum', 'ch', 'myv', 'ee', 'jbo', 'lg', 'mni',
                  'mnw', 'nrm', 'pdc', 'rmy', 'sm', 'trv', 'shn', 'tyv', 'kcg', 'bm',
                  'gom', 'wo', ' anp', 'arc', 'din', 'got', 'kbd', 'xal', 'szy', 'stq',
                  'sn', 'to', 'map-bms', 'ny', 'chy', 'glk', 'inh', 'jam', 'nqo', 'ti',
                  'ts', 'tay', 'kl', 'kg', 'nia', 'pih', 'nso', 'pfl', 'alt', 'ty',
                  'bxr', 'cr', 'ltg', 'pag', 'pnt', 'za', 'ksh', 'fat', 'pcm', 'tn',
                  've', 'fj', 'koi', 'gcr', 'iu', 'ki', 'roa-rup', 'guw',
                  'krc', 'rn', 'kbp', 'pwn', 'pi', 'sg', 'ik', 'srn', 'lbe']
        
        self.LANGUAGE_CODES_BETWEEN_COLONS = [(":"+x+":") for x in self.LANGUAGE_CODES]

        self.IGNORE_THE_EXTRA_CONTENT = ['[[File:', '[[Category:', '[[Image:']
        self.IGNORE_SECTION = ['reference', 'references',
                               'see also',
                               'completed',
                               'track listing',
                               'gallery']
        self.IGNORE_REDIRECTS = ['#REDIRECT', '#redirect', '#Redirect']
        self.ENTITIES = ['&nbsp;', '&lt;', '&gt;', '&amp;', '&quot;',	
        '&apos;', '&cent;', '&pound;', '&yen;' '&euro;', '&copy;', '&reg;']

        #OUTPUT FILES
        self.output_files = [x for x in 'abcdefghijklmnopqrstuvwxyz0123456789']

        logging.info('Initialization completed!')

    def split_page(self, chunk=False):

        #Splits the entire page into smaller pages.

        """
        
        :params chunk (bool): For debugging purpose. False (default).

        """

        CHUNK_SIZE = 50

        if chunk:
            _ = sample(self.elements, CHUNK_SIZE)
            self.length = CHUNK_SIZE
        else:
            _ = self.elements
        for element in _:

            title = element.getElementsByTagName('title')[0].childNodes[0].nodeValue
            text = element.getElementsByTagName('text')[0].childNodes[0].nodeValue
            ns = element.getElementsByTagName('ns')[0].childNodes[0].nodeValue
            id = element.getElementsByTagName('id')[0].childNodes[0].nodeValue

            if any(redirect in text for redirect in self.IGNORE_REDIRECTS):
                continue
            else:
                #Get the text, nothing else.
                if ns == '0':
                    #self.titles.append(title)
                    #self.texts.append(text)
                    #self.ids.append(id)

                    self.extraction.update({id:[title, text]})
                else:
                    pass

        logging.info('XML has splitted, total page number is: %d', self.length)
              
        return self.extraction

    #This is for debugging purpose.
    def check_by_id(self, xid):
        
        for idx, title_text in self.extraction.items():
            if xid == idx:
                logging.info('Checked by ID: %s', title_text[1])

    def clean_by_id(self, xid):

        for idx, title_text in self.extraction.items():
            if xid == idx:
                logging.info('Cleaning by ID: %s', xid)
                temp_dict = self.cleaning_text(chunk=False, by_id=True, one_id = xid)
                logging.info('Cleaned data: %s', temp_dict)

    def cleaning_text(self, chunk=False, by_id=False, one_id=0):

        """
        :params chunk (bool): If True, randomly extracts pages by chunk size. For debugging purposes. (default: False)
        :params by_id (bool): If True, cleans one page based on its id and returns it. (default: False)
        :params one_id: The id of the pages we're gonna clean. If by_id = False, this does not have effect.

        :output plain_text (dict): {'id': ['title', 'text']}
        """

        self.plain_text.clear()
        ids, titles, texts = ([] for x in range(3))

        if by_id:

            titles.append(self.extraction[one_id][0])
            texts.append(self.extraction[one_id][1])
            ids.append(one_id)

        else:    

            _ = self.split_page()
            ids = list(_.keys())[:]
            titles = [d[0] for d in _.values()][:]
            texts = [d[1] for d in _.values()][:]
        print("PART OF TITLES: ",titles[:5])
        print("PART OF IDS: ", ids[:5])
        for title, id, text in zip(titles, ids, texts):

            #We're gonna remove the HTML-like tags.

            text = re.sub("<!-+(.*?)-+>", '', text, flags=re.DOTALL)
            #text = re.sub(r'<.*?>', '', text)

            #Remove the part after 'IGNORE_SECTION'.

            _id = []
            for part in re.findall(r'=+(.*?)=+', text):
                if part.lower().strip() in self.IGNORE_SECTION:
                    _id.append(text.find(part))
                else:
                    continue

            if len(_id) != 0:
                text = text[:min(_id)]

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
            for extra in self.IGNORE_THE_EXTRA_CONTENT:
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

            #We're gonna remove the wandering brackets.
            text = text.replace('[[','').replace(']]','')
            
            #Then we're gonna remove all the links in the text.
            #text = re.sub(r'http+', '', text) #p\S
            text = re.sub(r'https?:\/\/.*[\r\n]*', '', text, flags=re.MULTILINE)
            
            #Remove headlines, we don't need them.
            regex_symbol = re.compile(r'=+(.*?)=+')
            text = re.sub(regex_symbol, '', text)

            #Remove the special characters, not cool.    

            for entity in self.ENTITIES:
                special = re.compile(entity)
                text = re.sub(special, '', text)

            #Remove the symbols we don't need.

            symbols = ["'", "{", "}", "*", '#', '"', "()", "[", "]"]
            for symbol in symbols:
                text = text.replace(symbol, '')

            #Some additional regex for wikipedia format.
            one = re.compile(r'\n\|.*\n')
            text = re.sub(one, "", text)

            two = re.compile(r'\|+(.*?)\|+')
            text = re.sub(two, "", text)

            three = re.escape('align=')+r'.*\n'
            text = re.sub(three, "", text)
            
            #New line and space remover, the data must be plain text.
            line_regex = re.compile(r'\n+')
            space_regex = re.compile(r'\s\s+')

            text = re.sub(line_regex, ' ', text)
            text = re.sub(space_regex, ' ', text)

            #Removing the meaningless tags.
            text = text.replace('&ndash;', '-')
            
            #Removing the trailing spaces, if there is any.
            text = text.lstrip(' ').rstrip(' ')

            title = title
            self.plain_text.update({title:[id,text]})
        return self.plain_text
    
    def write_out(self, _json=True):

        #Get the dictionary.
        out = self.cleaning_text()

        #Example: /Users/user/Desktop/project/bundle
        root_path = 'bundle'
        output_path = os.path.join(self.dir_path, root_path)
        
        #ALL TITLES, IDS, TEXTS from the dictionary copied below.
        _titles = list(out.keys())[:]
        _ids = [d[0] for d in out.values()]
        _texts = [d[1] for d in out.values()]

        #This list will be used to check if the 'page' name starts with a numerical value.
        #[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        #_numeric = [str(x) for x in range(0,10)]

        #Iterations begins with the first letter of 'a' till the end of numericals which is '9'.
        for i in self.output_files:

            #We generate file names for each initial.
            path_json = output_path + '/' + i + '.json'
            path_csv = output_path + '/' + i + '.csv'

            #id_list for the letter we match for each iteration.
            id_list = [y for x, y in zip(_titles, _ids) if x[0].lower() == i] 

            #Output file, will be written out.
            TEMP_DICT = {}

            #Check every page and add it into the 'TEMP_DICT' if the id has a match in our id_list.
            for dt, di, de in zip(_titles, _ids, _texts):
                if di in id_list:
                    TEMP_DICT.update({dt:[di, de]})

            #Check if our directory contains the 'root path' which is 'bundle' in this case. Make sure it's there.
            if root_path not in os.listdir(self.dir_path):
                os.makedirs(os.path.join(self.dir_path, root_path))
            else:
                pass    

            if _json:
                with open(path_json, 'w') as j:
                    json.dump(TEMP_DICT, j, indent=2, ensure_ascii=False)
                    j.write('\n')
            else:
                with open(path_csv, 'w', newline='') as f:
                    w = csv.DictWriter(f, TEMP_DICT.keys())
                    w.writeheader()
                    w.writerow(TEMP_DICT)
            logging.info('Dump was successful to %s', output_path)
            
            TEMP_DICT.clear() 
  
def main():    
    start = perf_counter()
    App = WikiText(input_file)
    #extracted_file = App.cleaning_text()
    #App.write_out(_json=True)
    App.write_out()
    end = perf_counter()

    print("Process took %.2f seconds.", end-start)

    logging.info("Process took %.2f seconds.", end-start)


if __name__ == '__main__':
    main()