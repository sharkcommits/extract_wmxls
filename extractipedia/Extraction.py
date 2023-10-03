#!/usr/bin/env python
# -*- coding: utf-8 -*-

#     =======================================================================
# 
#     Copyright (c) 2023  @sharkcommits (sharkcommits@protonmail.com)
# 
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
# 
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
# 
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#     INSTALLATION: 'pip install extractipedia'
#
#     USAGE: 'python -m extractipedia.Extraction -f YOUR_FILE.xml'
#
#     HELP: 'python -m extractipedia.Extraction --help' to get more information.
#
#     =======================================================================

from xml.etree.ElementTree import iterparse
from time import perf_counter
import multiprocessing
from gc import collect
from extractipedia.utils import *
import datetime
import argparse
import queue

__version__ = '0.0.1'

class WikiText:

    def __init__(self, file, batch_size):
        self.file = file
        self.batch_size = batch_size

        self.current_batch = []
        self.current_batch_size = 0

        self.xml_parser = None

    @classmethod
    def strip_that(self, string):
        idx = string.rfind('}')
        if idx != -1:
            string = string[idx + 1:]
        return string  

    def __iter__(self):
        return self

    def __next__(self):

        articles_done = 0

        if self.xml_parser is None:
            self.xml_parser = iterparse(self.file, events=("start", "end"))

        for event, element in self.xml_parser:
            flag = self.strip_that(element.tag)

            if event == 'start':
                if flag == 'page':
                    title, redirect = '', ''
                    id = -100
                    revision = False
                    ns = 0
                elif flag == 'revision':
                    revision = True
            else:
                if flag == 'title':
                    title = element.text   
                elif flag == 'text':
                    text = element.text
                elif flag == 'id' and not revision:
                    id = int(element.text)
                elif flag == 'redirect':
                    redirect = element.attrib['title']
                elif flag == 'ns':
                    ns = int(element.text)
                elif flag == 'page':
                    try:
                        if ns == 0:
                            if len(redirect) == 0:
                                if 'may refer to' in text:
                                    pass
                                else:
                                    articles_done += 1
                                    TEMP_DICT = {title:[id, text]}
                                    self.current_batch.append(TEMP_DICT)
                                    self.current_batch_size += 1
                                    del TEMP_DICT 
                    except Exception as e:
                        print(e) 

                    title, text, redirect = '', '', ''
                    ns = -100     
                    element.clear() 

                if self.current_batch_size >= self.batch_size:
                    # Yield the current batch as a single string.
                    batch_to_yield = self.current_batch.copy()
                    self.current_batch.clear()
                    _ = collect()
                    self.current_batch_size = 0
                    return batch_to_yield, articles_done

        # Yield any remaining data in the last batch.
        if self.current_batch:
            batch_to_yield = self.current_batch.copy()
            self.current_batch.clear()
            _ = collect()
            self.current_batch_size = 0
            return batch_to_yield, articles_done
        
        raise StopIteration

SENTINEL = 0

def process_batch(batch, database_file, table_name, first_sentence, pages):

    """
    :params batch: chunks of data to be written to the database.
    :params database_file: name of the database.
    :params table_name: name of the table in the database.
    :params pages: page count for every process.
    """
    global SENTINEL
    if pages > SENTINEL:
        SENTINEL = pages
    logging.info(f'{SENTINEL} articles have been updated.')
    result = cleaning_text(batch, first_sentence)
    update_sqlite_table_with_dict(database_file, table_name, result)

class BatchProcessor:

    def __init__(self, max_queue_size, process_func, num_workers, database_file, table_name, first_sentence):
        self.max_queue_size = max_queue_size
        self.batch_queue = multiprocessing.Queue(maxsize=max_queue_size)
        self.process_func = process_func
        self.num_workers = num_workers
        self.stop_event = multiprocessing.Event()

        self.database_file = database_file
        self.table_name = table_name

        self.first_sentence = first_sentence

        self.page_count = 0

    def process_batches(self, data_generator):
        producer_process = multiprocessing.Process(target=self._produce_batches, args=(data_generator,))
        consumer_processes = [multiprocessing.Process(target=self._consume_batches, args=(TOTAL_ARTICLES,)) for _ in range(self.num_workers)]

        producer_process.start()
        for consumer_process in consumer_processes:
            consumer_process.start()

        producer_process.join()
        for consumer_process in consumer_processes:
            consumer_process.join()

    def _produce_batches(self, data_generator):
        for item, pages in data_generator:
            combined = (item, pages)
            self.batch_queue.put(combined)

        # Signal that there are no more items to be added to the queue.
        for _ in range(self.num_workers):
            self.batch_queue.put(None)

    def _consume_batches(self, TOTAL_ARTICLES):
        while not self.stop_event.is_set():
            try:
                result = self.batch_queue.get(timeout=1)
                batch, pages = result if isinstance (result, tuple) else (result, 0)
                if batch is None:
                    break  # Exit the loop when there is no more batches.

                TOTAL_ARTICLES.value += pages

                # Process the batch using the provided function.
                self.process_func(batch, self.database_file, self.table_name, self.first_sentence, TOTAL_ARTICLES.value)
            except queue.Empty:
                pass
            except Exception as e:
                print(f"Error: {e}")

    def stop(self):
        self.stop_event.set()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Extractipedia')
    parser.add_argument("-f", "--file_name", type=str, help="XML File Name")
    parser.add_argument("-b", "--batch_size", type=int, default=2500, help="Batch Size (default=2500)")
    parser.add_argument("-d", "--database_file", type=str, default='new_database.db', help="Database Name (default=new_database.db)")
    parser.add_argument("-t", "--table_name", type=str, default='new_table', help="Table Name (default=new_table)")
    num_workers = multiprocessing.cpu_count() - 1
    max_workers = multiprocessing.cpu_count()
    parser.add_argument("-n", "--num_workers", type=int, default=num_workers, help=f"Number of Workers: (default={num_workers}, max={max_workers}). How many cores do you want to use? It is advisable that you should at least exclude 1 core in order to give your machine breathing room. Max is the core number your machine has. You can simply type the number of cores to be used.")
    parser.add_argument("-s", "--first_sentence", action="store_true", default=False, help="If you include [-s] while executing the script, it'll only write out the first sentences of the pages to the database. Useful for basic indexes. Reduce memory by more than 90 percent!")
    args = parser.parse_args()

    logging.info(f'File Name: {args.file_name}')
    logging.info(f'Batch Size: {args.batch_size}')
    logging.info(f'Database: {args.database_file}. Table: {args.table_name}')
    logging.info(f'Processors in Use: {args.num_workers}')
    logging.info(f'First Sentence? {args.first_sentence}')

    start_time = perf_counter()

    TOTAL_ARTICLES = multiprocessing.Value('i', 0)

    max_queue_size = 5

    batch_processor = BatchProcessor(max_queue_size, process_func=process_batch, num_workers=args.num_workers, database_file=args.database_file, table_name=args.table_name, first_sentence=args.first_sentence)
    generator = WikiText(args.file_name, args.batch_size)

    batch_processor.process_batches(generator)

    bed_time = perf_counter()

    time_difference = round(bed_time - start_time)
    formatted_time = str(datetime.timedelta(seconds=time_difference))

    logging.info(f'It is done in {formatted_time} (hh:mm:ss).')