#!/usr/bin/env python
# -*- coding: utf-8 -*-

#     =======================================================================
# 
#     Copyright (c) 2023  @sharkcommits (sharkcommits@protonmail.ch)
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
#     INSTALLATION: 'pip3 install extractipedia'
#
#     USAGE: 'python3 extractipedia.py -f YOUR_FILE.xml'
#
#     HELP: 'python3 extractipedia.py --help' to get more information.
#
#     =======================================================================


"""

This file prints the first n items from the database like the following section:

('title1', [id1, 'text1'])
('title2', [id2, 'text2'])
('title3', [id3, 'text3'])

"""
import argparse
from src.util import retrieve_data_from_sqlite

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Reading Database')
    parser.add_argument("-d", "--database_file", type=str, default='new_database.db')
    parser.add_argument("-t", "--table_name", type=str, default='new_table')
    parser.add_argument("-c", "--chunk", type=int, default=10)
    args = parser.parse_args()

    try:

        result = list(retrieve_data_from_sqlite(database_file=args.database_file, table_name=args.table_name).items())[:args.chunk]
        
        for item in result:
            print(item)

    except Exception as e:

        print(e)        