import pandas as pd
import xml.dom.minidom
import re
from multiprocessing import Queue, get_context, cpu_count
import logging
from timeit import default_timer
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
        self.domtree = xml.dom.minidom.parse(self.file)
        self.group = self.domtree.documentElement
        self.elements = self.group.getElementsByTagName('page')

        self.dir_path = os.path.dirname(os.path.realpath(__file__))

        self.titles = []
        self.texts = []
        self.ids = []
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
        self.IGNORE_SECTION = ['reference', 'Reference', 'References', 'references', 'REFERENCE', 'REFERENCES',
                               'see also', 'See also', 'See Also', 'SEE ALSO', 'see Also',
                               'completed', 'Completed', 'COMPLETED']
        self.IGNORE_REDIRECTS = ['#REDIRECT', '#redirect', '#Redirect']
        self.ENTITIES = ['&nbsp;', '&lt;', '&gt;', '&amp;', '&quot;',	
        '&apos;', '&cent;', '&pound;', '&yen;' '&euro;', '&copy;', '&reg;']

        print('Initialization completed!')

    def split_page(self, chunk=False):

        CHUNK_SIZE = 9

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
                #if ('File:' not in title) and ('Article:' not in title) and ('Category:' not in title) and ('Wikipedia:' not in title) and ('Template:' not in title):
                    self.titles.append(title)
                    self.texts.append(text)
                    self.ids.append(id)

                    self.extraction.update({id:[title, text]})
                else:
                    pass

        print('XML has splitted, total page number is: ', self.length)
              
        return self.titles, self.ids, self.texts

    #This is for debugging purpose.
    def check_by_id(self, xid):
        
        for idx, title_text in self.extraction.items():
            if xid == idx:
                print("Checked by ID: ", title_text[1])

    def clean_by_id(self, xid):

        for idx, title_text in self.extraction.items():
            if xid == idx:
                print('Cleaning by ID: ', xid)
                temp_dict = self.cleaning_text(chunk=False, by_id=True, one_id = xid)
                print(temp_dict)

    def cleaning_text(self, chunk=False, by_id=False, one_id=0):

        """
        :params chunk (bool): If True, randomly extracts pages by chunk size. For debugging purposes. (default: False)
        :params by_id (bool): If True, cleans one page based on its id and returns it. (default: False)
        :params one_id: The id of the pages we're gonna clean. If by_id = False, this does not have effect.

        :output plain_text (dict): {'id': ['title', 'text']}
        """

        titles = []
        texts = []
        ids = []

        plain_text = {}

        if by_id:

            titles.append(self.extraction[one_id][0])
            texts.append(self.extraction[one_id][1])
            ids.append(one_id)
        else:    
            
            titles, ids, texts = self.split_page(chunk)

        for title, id, text in zip(titles, ids, texts):

            # ==CURLY BRACKET REMOVER== # 

            idx = -1 #Starting from -1 in order to begin indexing from 0 at the beginning of our for loop. "idx += 1"
            open_curly = 0 #Open curly count
            closed_curly = 0 #Closed curly count
            open_idx = {} #Open curlies and indexes.
            closed_idx = {} #Closed curlies and indexes.
            
            _seperated = []
            _equals = []
            
            SEPERATOR = False
            
            for letter in text:
                idx += 1
                if letter == '{':
                    open_curly += 1 #Incrementing the open_curly.
                    open_idx.update({open_curly:idx})
                    SEPERATOR = True
                elif letter == '}':
                    closed_curly += 1 #Incrementing the closed_curly.
                    closed_idx.update({closed_curly:idx})
                    SEPERATOR = False
                else:
                    continue
                #For getting the same nested curly bracket.
                if open_curly == closed_curly:
                    _equals.append(idx)
                    SEPERATOR = False
                
                elif (open_curly == closed_curly + 1) and SEPERATOR:
                    _seperated.append(idx)
                else:
                    continue
            
            #Removing the curly bracket sections and the text inside.
            _temp = list(text)
            for k, v in zip(_seperated[::-1], _equals[::-1]):
                start_idx = k
                end_idx = v+1
                del _temp[start_idx:end_idx]
            text = ''.join(_temp)
            
            #Checks the page and removes extra contents such as files and categories.
            for extra in self.IGNORE_THE_EXTRA_CONTENT:
                _ = 0
                _start = []
                _end = []
                while _ < len(text):
                    index = text.find(extra, _)
                    if index == -1:
                        break
                    _start.append(index-2)
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
            
            #We're gonna remove the HTML-like tags.
            text = re.sub("<!-+(.*?)-+>", '', text, flags=re.DOTALL)
            #regex_ref = r'<'+re.escape('ref')+r'>.*?<'+re.escape('/ref')+r'>'
            #text = re.sub(regex_ref, '',text)
            text = re.sub("<(.*?)>(.*?)</(.*?)>", '', text, flags=re.DOTALL)
            text = re.sub(r'<.*?>', '', text)
            #text = re.sub("<(.*?)>(.|\n)*?</(.*?)>", '', text)

            #We're gonna remove the wandering brackets.
            text = text.replace('[[','').replace(']]','')
            
            #Then we're gonna remove all the links in the text.
            #text = re.sub(r'http+', '', text) #p\S
            text = re.sub(r'https?:\/\/.*[\r\n]*', '', text, flags=re.MULTILINE)
            
            #Remove the part after 'IGNORE_SECTION'.

            _id = []
            for part in re.findall(r'=+(.*?)=+', text):
                if part.lower().strip() in self.IGNORE_SECTION:
                    _id.append(text.find(part))
                else:
                    continue

            if len(_id) != 0:
                text = text[:min(_id)]
            
            #Remove headlines, we don't need them.
            regex_symbol = re.compile(r'=+(.*?)=+')
            text = re.sub(regex_symbol, '', text)

            #Remove the special characters, not cool.    

            for entity in self.ENTITIES:
                special = re.compile(entity)
                text = re.sub(special, '', text)

            #Remove the symbols we don't need.

            symbols = ["'", "{", "}", "*", '"', "(", ")", "[", "]"]
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
            space_regex = re.compile(r'\s+')

            text = re.sub(line_regex, ' ', text)
            text = re.sub(space_regex, ' ', text)
            
            #Removing the trailing spaces, if there is any.
            text = text.lstrip(' ').rstrip(' ')

            
            title = title
            self.plain_text.update({title:[id,text]})
        return self.plain_text
    
    def write_out(self, _json=False):

        file_path_csv = self.dir_path + '/plain.csv'
        file_path_json = self.dir_path + '/plain.json'
        if _json:

            with open(file_path_json, 'w') as j:
                json.dump(self.plain_text, j, indent=2, ensure_ascii=False)
                j.write('\n')
        else:

            with open(file_path_csv, 'w', newline='') as f:
                w = csv.DictWriter(f, self.plain_text.keys())
                w.writeheader()
                w.writerow(self.plain_text)

def main():    
    App = WikiText(input_file)
    extracted_file = App.cleaning_text()
    App.write_out(_json=True)


if __name__ == '__main__':
    main()