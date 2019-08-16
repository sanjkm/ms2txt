#!/usr/bin/env python
"""
Function that takes a directory as input, converts all Metastock binary
data in the directory into a Pandas dataframe, returns the dataframe
"""

import sys
import pandas as pd

from metastock.files import MetastockFiles
from metastockX.mod_files import MSEMasterFile


# default is set to extract all data in the directory
class set_options():
    def __init__(self, ticker_list, decimal_precision):

        self.all = True
        if len(ticker_list) > 0:
            self.all = False
    
        self.precision = decimal_precision
        self.encoding = 'ascii'

def ms2pandas(dir_path='', decimal_precision=4, ticker_list=[]):

    options = set_options(ticker_list, decimal_precision) # default values    
    args = ticker_list

    if len(dir_path) > 0 and dir_path[-1] != '/':
        dir_path += '/'
    
    em_file = MetastockFiles(options.encoding, options.precision, dir_path)
    
    Xem_file = MSEMasterFile('XMASTER', options.precision, dir_path)
    
    # extract the data into lists of dictionaries
    data_list = em_file.output_data_list(options.all, args)
    data_listX = Xem_file.output_data_list(options.all, args, dir_path)
    
    df = pd.DataFrame(data_list + data_listX)

    return df
