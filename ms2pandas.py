#!/usr/bin/env python
"""
Function that takes a directory as input, converts all Metastock binary
data in the directory into a Pandas dataframe, returns the dataframe
"""

import sys

from metastock.files import MetastockFiles

Usage = """usage: %prog [options] [symbol1] [symbol2] ....

Examples:
    %prog -p 2 --all        extract all symbols from EMASTER file
    %prog FW20 "S&P500"     extract FW20 and S&P500 from EMASTER file
"""

# default is set to extract all data in the directory
class set_options():
    def __init__(self, ticker_list, decimal_precision):

        self.all = True
        if len(ticker_list) > 0:
            self.all = False
    
        self.precision = decimal_precision
        self.encoding = 'ascii'

def ms2pandas(dir_path, decimal_precision=4, ticker_list=[]):

    options = set_options(ticker_list, decimal_precision) # default values    
    args = ticker_list

    em_file = MetastockFiles(options.encoding, options.precision)
    
    # extract the data
    em_file.output_ascii(options.all, args)

ms2pandas('', decimal_precision=4, ticker_list=[])
