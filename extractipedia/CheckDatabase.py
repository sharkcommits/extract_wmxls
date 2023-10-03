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
#     USAGE: 'python -m extractipedia.CheckDatabase -f YOUR_DATABASE.db -t YOUR_TABLE'
#
#     HELP: 'python -m extractipedia.CheckDatabase --help' to get more information.
#
#     =======================================================================


"""

This script prints the first n items from the database like the following section:

('title_1', [id_1, 'text_1'])
('title_2', [id_2, 'text_2'])
('title_n', [id_n, 'text_n'])

"""
import gc
import argparse
from .utils import retrieve_data_from_sqlite

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Reading Database')
    parser.add_argument("-d", "--database_file", type=str, default='new_database.db')
    parser.add_argument("-t", "--table_name", type=str, default='new_table')
    parser.add_argument("-c", "--chunk_size", type=int, default=10, help='Retrieve n pages from the database.')
    parser.add_argument("-r", "--random", action="store_true", default=False, help='Selects random n pages. (default=False)')
    args = parser.parse_args()

    try:

        result = retrieve_data_from_sqlite(args.database_file, args.table_name, args.chunk_size, args.random)
        
        for key, value in result.items():
            print('='*10 + key + '='*10, value, sep='\n')

        del result
        _ = gc.collect()    

    except Exception as e:

        print(e)        