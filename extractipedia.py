import queue
from xml.etree.ElementTree import iterparse
from util import *
from time import perf_counter
from gc import collect
import multiprocessing
from multiprocessing import cpu_count
import argparse

class WikiText:

    def __init__(self, file, batch_size):
        self.totalCount = 0
        self.articleCount = 0
        self.redirectCount = 0
        self.file = file
        self.batch_size = batch_size

        self.current_batch = []
        self.current_batch_size = 0

        self.xml_parser = None

    def tag(self, string):
        idx = k = string.rfind('}')
        if idx != -1:
            string = string[idx + 1:]
        return string  

    def __iter__(self):
        return self

    def __next__(self):

        is_first = True

        if self.xml_parser is None:
            self.xml_parser = iterparse(self.file, events=("start", "end"))

        for event, element in self.xml_parser:
            tagName = self.tag(element.tag)

            if is_first:
                is_first = False
            if event == 'start':
                if tagName == 'page':
                    title = ''
                    id = -1
                    redirect = ''
                    inrevision = False
                    ns = 0
                elif tagName == 'revision':
                    inrevision = True
            else:
                
                if tagName == 'title':
                    title = element.text   
                elif tagName == 'text':
                    text = element.text
                elif tagName == 'id' and not inrevision:
                    id = int(element.text)
                elif tagName == 'redirect':
                    redirect = element.attrib['title']
                elif tagName == 'ns':
                    ns = int(element.text)
                elif tagName == 'page':
                    self.totalCount += 1 
                    try:
                        if ns == 0:
                            if len(redirect) > 0:
                                self.redirectCount += 1
                            else:
                                if 'may refer to' in text:
                                    pass
                                else:
                                    self.articleCount += 1
                                    TEMP_DICT = {title:[id, text]}
                                    self.current_batch.append(TEMP_DICT)
                                    self.current_batch_size += 1
                                    del TEMP_DICT 
                    except Exception as e:
                        print(e) 

                    title = ''
                    redirect = ''
                    text = ''
                    ns = -1      
                    element.clear() 

                if self.current_batch_size >= self.batch_size:
                    # Yield the current batch as a single string
                    batch_to_yield = self.current_batch.copy()
                    self.current_batch.clear()
                    _ = collect()
                    self.current_batch_size = 0
                    return batch_to_yield


        # Yield any remaining data in the last batch
        if self.current_batch:
            batch_to_yield = self.current_batch.copy()
            self.current_batch.clear()
            _ = collect()
            self.current_batch_size = 0
            return batch_to_yield
        
        raise StopIteration

def process_batch(batch, database_file, table_name, first_sentence):
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

    def process_batches(self, data_generator):
        producer_process = multiprocessing.Process(target=self._produce_batches, args=(data_generator,))
        consumer_processes = [multiprocessing.Process(target=self._consume_batches) for _ in range(self.num_workers)]

        producer_process.start()
        for consumer_process in consumer_processes:
            consumer_process.start()

        producer_process.join()
        for consumer_process in consumer_processes:
            consumer_process.join()

    def _produce_batches(self, data_generator):
        for item in data_generator:
            self.batch_queue.put(item)

        # Signal that there are no more items to be added to the queue
        for _ in range(self.num_workers):
            self.batch_queue.put(None)

    def _consume_batches(self):
        while not self.stop_event.is_set():
            try:
                batch = self.batch_queue.get(timeout=1)
                if batch is None:
                    break  # Exit the loop when sentinel value is received

                # Process the batch using the provided function
                self.process_func(batch, self.database_file, self.table_name, self.first_sentence)
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
    num_workers = cpu_count() - 2
    max_workers = cpu_count()
    parser.add_argument("-w", "--num_workers", type=int, default=num_workers, help=f"Number of Workers: (default={num_workers}, max={max_workers}). How many cores do you want to use? It is advisable that you should at least exclude 1 core in order to give your machine breathing room. Max is the core number your machine has.")
    parser.add_argument("-s", "--first_sentence", type=bool, default=False, help="True, if you need just the first sentence. (default=False)")
    args = parser.parse_args()

    logging.info(f'File Name: {args.file_name}')
    logging.info(f'Number of Workers: {args.num_workers}')
    logging.info(f'Batch Size: {args.batch_size}')
    logging.info(f'Database Name: {args.database_file}')
    logging.info(f'Table Name: {args.table_name}')
    logging.info(f'First Sentence? -{args.first_sentence}')

    start_time = perf_counter()

    max_queue_size = 12

    batch_processor = BatchProcessor(max_queue_size, process_func=process_batch, num_workers=args.num_workers, database_file=args.database_file, table_name=args.table_name, first_sentence=args.first_sentence)
    generator = WikiText(args.file_name, args.batch_size)

    batch_processor.process_batches(generator)

    bed_time = perf_counter()

    logging.info(f'It took exactly {bed_time-start_time:.2f} seconds.')