"""
Reading metastock files.
"""
import os
import struct
import re
import math
import traceback

from .utils import fmsbin2ieee, float2date, float2time, convertSymbolName
from .utils import paddedString

class DataFileInfo(object):
    """
    I represent a metastock data describing a single symbol
    Each symbol has a number (file_num). To read the quotes we need to read
    two files: a <file_num>.DAT file with the tick data and a <file_num>.DOP
    file describing what columns are in the DAT file
    @ivar file_num: symbol number
    @ivar num_fields: number of columns in DAT file
    @ivar stock_symbol: stock symbol
    @ivar stock_name: full stock name
    @ivar time_frame: tick time frame (f.e. 'D' means EOD data)
    @ivar first_date: first tick date
    @ivar last_date: last tick date
    @ivar columns: list of columns names
    """
    file_num = None
    num_fields = None
    stock_symbol = None
    stock_name = None
    time_frame = None
    first_date = None
    last_date = None

    reg = re.compile('\"(.+)\",.+', re.IGNORECASE)
    columns = None

    def _load_columns(self, dir_path):
        """
        Read columns names from the DOP file
        """
        filename = (dir_path + 'F%d.DOP') % self.file_num # changed DOP to MWD
        file_handle = open(filename, 'r')
        lines = file_handle.read().split()
        file_handle.close()
        
        # assert((len(lines)-1) == self.num_fields)
        self.columns = []
        for line in lines[:-1]:
            match = self.reg.search(line)
            colname = match.groups()[0]
            self.columns.append(colname)


    class Column(object):
        """
        This is a base class for classes reading metastock data for a specific
        columns. The read method is called when reading a decode the column
        value
        @ivar dataSize: number of bytes is the data file that is used to store
                        a single value
        @ivar name: column name
        """
        dataSize = 4
        name = None

        def __init__(self, name):
            self.name = name

        def read(self, bytes):
            """Read and return a column value"""

        def format(self, value):
            """
            Return a string containing a value returned by read method
            """
            return str(value)

    class DateColumn(Column):
        """A date column"""
        def read(self, bytes):
            """Convert from MBF to date string"""
            return float2date(fmsbin2ieee(bytes))

        def format(self, value):
            if value is not None:
                return value.strftime('%Y%m%d')
            return DataFileInfo.Column.format(self, value)

    class TimeColumn(Column):
        """A time column"""
        def read(self, bytes):
            """Convert read bytes from MBF to time string"""
            return float2time(fmsbin2ieee(bytes))

        def format(self, value):
            if value is not None:
                return value.strftime('%Y%m%d')
            return DataFileInfo.Column.format(self, value)

    class FloatColumn(Column):
        """
        A float column
        @ivar precision: round floats to n digits after the decimal point
        """
        precision = 2

        def read(self, bytes):
            """Convert bytes containing MBF to float"""
            return fmsbin2ieee(bytes)

        def format(self, value):
            return ("%0."+str(self.precision)+"f") % value

    class IntColumn(Column):
        """An integer column"""
        def read(self, bytes):
            """Convert MBF bytes to an integer"""
            return int(fmsbin2ieee(bytes))

    # we map a metastock column name to an object capable reading it
    knownMSColumns = {
        'DATE': DateColumn('Date'),
        'TIME': TimeColumn('Time'),
        'OPEN': FloatColumn('Open'),
        'HIGH': FloatColumn('High'),
        'LOW': FloatColumn('Low'),
        'CLOSE': FloatColumn('Close'),
        'VOL': IntColumn('Volume'),
        'OI': IntColumn('Oi'),
    }
    unknownColumnDataSize = 4    # assume unknown column data is 4 bytes long

    max_recs = 0
    last_rec = 0

    def load_candles(self, dir_path):
        """
        Load metastock DAT file and write the content
        to a text file
        """
        file_handle = None
        outfile = None
        try:
            filename = (dir_path + 'F%d.MWD') % self.file_num  # Changed DAT to MWD
            file_handle = open(filename, 'rb')
            self.max_recs = struct.unpack("H", file_handle.read(2))[0]
            self.last_rec = struct.unpack("H", file_handle.read(2))[0]
            self.num_fields = len(self.columns)

            # not sure about this, but it seems to work
            file_handle.read((self.num_fields - 1) * 4)

            #print "Expecting %d candles in file %s. num_fields : %d" % \
            #    (self.last_rec - 1, filename, self.num_fields)

            outfile = open('%s.TXT' % self.stock_symbol, 'w')
            # write the header line, for example:
            #"Name","Date","Time","Open","High","Low","Close","Volume","Oi"
            outfile.write('"Name"')
            columns = []
            for ms_col_name in self.columns:
                column = self.knownMSColumns.get(ms_col_name)
                if column is not None:
                    outfile.write(',"%s"' % column.name)
                columns.append(column) # we append None if the column is unknown
            outfile.write('\n')

            # we have (self.last_rec - 1) candles to read
            for _ in range(self.last_rec - 1):
                outfile.write(self.stock_symbol)
                for col in columns:
                    if col is None: # unknown column?
                        # ignore this column
                        file_handle.read(self.unknownColumnDataSize)
                    else:
                        # read the column data
                        bytes = file_handle.read(col.dataSize)
                        # decode the data
                        value = col.read(bytes)
                        # format it
                        value = col.format(value)

                        outfile.write(',%s' % value)

                outfile.write('\n')
        finally:
            if outfile is not None:
                outfile.close()
            if file_handle is not None:
                file_handle.close()
                
    # Upload candle values into a list of dictionaries
    # Eventually, the list will be converted to pandas dataframe
    def candles_to_list(self, dir_path):
        
        file_handle = None
        outfile = None
        data_dict_list = []
        
        try:
            filename = (dir_path + 'F%d.MWD') % self.file_num  # Changed DAT to MWD
            file_handle = open(filename, 'rb')
            self.max_recs = struct.unpack("H", file_handle.read(2))[0]
            self.last_rec = struct.unpack("H", file_handle.read(2))[0]
            self.num_fields = len(self.columns)

            # not sure about this, but it seems to work
            file_handle.read((self.num_fields - 1) * 4)

            #print "Expecting %d candles in file %s. num_fields : %d" % \
            #    (self.last_rec - 1, filename, self.num_fields)

            # outfile = open('%s.TXT' % self.stock_symbol, 'w')
            # write the header line, for example:
            #"Name","Date","Time","Open","High","Low","Close","Volume","Oi"
            # outfile.write('"Name"')
            columns = []
            for ms_col_name in self.columns:
                column = self.knownMSColumns.get(ms_col_name)
                if column is not None:
                    pass
                    # outfile.write(',"%s"' % column.name)
                columns.append(column) # we append None if the column is unknown
            # outfile.write('\n')

            # we have (self.last_rec - 1) candles to read
            for _ in xrange(self.last_rec - 1):
                data_dict = {}
                data_dict['Symbol'] = self.stock_symbol
                # outfile.write(self.stock_symbol)
                for col in columns:
                    if col is None: # unknown column?
                        # ignore this column
                        file_handle.read(self.unknownColumnDataSize)
                    else:
                        # read the column data
                        bytes = file_handle.read(col.dataSize)
                        # decode the data
                        value = col.read(bytes)
                        # format it
                        value = col.format(value)
                        data_dict[col.name] = value
                        
                        # outfile.write(',%s' % value)
                data_dict_list.append(data_dict)
                # outfile.write('\n')
        finally:
            '''
            if outfile is not None:
                outfile.close()
            '''
            if file_handle is not None:
                file_handle.close()
            return data_dict_list
    def convert2ascii(self, dir_path):
        """
        Load Metastock data file and output the data to text file.
        """
        print ("Processing %s (fileNo %d)" % (self.stock_symbol, self.file_num))
        try:
            #print self.stock_symbol, self.file_num
            self._load_columns(dir_path)
            #print self.columns
            self.load_candles(dir_path)

        except Exception:
            print ("Error while converting symbol", self.stock_symbol)
            traceback.print_exc()


    def convert2list(self, dir_path):
        """
        Load Metastock data file and output the data to list of dictionaries
        """
        # print "Processing %s (fileNo %d)" % (self.stock_symbol, self.file_num)
        try:
            #print self.stock_symbol, self.file_num
            self._load_columns(dir_path)
            #print self.columns
            data_dict_list = self.candles_to_list(dir_path)

        except Exception:
            print ("Error while converting symbol", self.stock_symbol)
            traceback.print_exc()
        finally:
            return data_dict_list

class MSEMasterFile(object):
    """
    Metastock extended index file
    @ivar stocks: list of DataFileInfo objects
    """
    stocks = None

    def _read_file_info(self, file_handle):
        """
        read the entry for a single symbol and return a DataFileInfo
        describing it
        @parm file_handle: emaster file handle
        @return: DataFileInfo instance
        """
        dfi = DataFileInfo()
        file_handle.read(1)
        dfi.stock_symbol = file_handle.read(15)

        dfi.stock_symbol = convertSymbolName (paddedString(dfi.stock_symbol,
                                                           'ascii'))
        
        dfi.stock_name = file_handle.read(46)

        dfi.time_frame = file_handle.read(1)
        file_handle.read(2)
        
        dfi.file_num = struct.unpack("H", file_handle.read(2))[0]
        file_handle.read(3)
        
        file_handle.read(1)
        file_handle.read(9)

        file_handle.read(24)
        dfi.first_date = (struct.unpack("i", \
                                                   file_handle.read(4))[0])
        dfi.next_date = (struct.unpack("i", \
                                                   file_handle.read(4))[0])
        file_handle.read(4)
        dfi.last_date = (struct.unpack("i", \
                                                   file_handle.read(4))[0])


        file_handle.read(30)
        return dfi

    def __init__(self, filename, precision=None, dir_path=''):
        """
        The whole file is read while creating this object
        @param filename: name of the file to open (usually 'EMASTER')
        @param precision: round floats to n digits after the decimal point
        """
        if precision is not None:
            DataFileInfo.FloatColumn.precision = precision

        self.dir_path = dir_path
        self.master_file = True
            
        if os.path.isfile(self.dir_path + 'XMASTER') == False: # no XMASTER file
            self.master_file = False
            return None
                    
        file_handle = open(dir_path + filename, 'rb')
        file_handle.read(10)


        files_no = struct.unpack("H", file_handle.read(2))[0]
        file_handle.read(2)
        last_file = struct.unpack("H", file_handle.read(2))[0]
        file_handle.read(2)
        final_file = struct.unpack("H", file_handle.read(2))[0]
        file_handle.read(2)
        file_handle.read(128)
        

        self.stocks = []

        while files_no > 0:
            self.stocks.append(self._read_file_info(file_handle))
            files_no -= 1
 
        file_handle.close()

    def list_all_symbols(self):
        """
        Lists all the symbols from metastock index file and writes it
        to the output
        """
        print ("List of available symbols:")
        for stock in self.stocks:
            print ("symbol: %s, name: %s, file number: %s" % \
                (stock.stock_symbol, stock.stock_name, stock.file_num))

    def output_ascii(self, all_symbols, symbols, dir_path=''):
        """
        Read all or specified symbols and write them to text
        files (each symbol in separate file)
        @param all_symbols: when True, all symbols are processed
        @type all_symbols: C{bool}
        @param symbols: list of symbols to process
        """
        if self.master_file == False: # no XMASTER file
            return None
        for stock in self.stocks:
            if all_symbols or (stock.stock_symbol in symbols):
                stock.convert2ascii(dir_path)

    def output_data_list (self, all_symbols, symbols, dir_path=''):
        """
        Read all or specified symbols and write them to list of dictionaries
        @param all_symbols: when True, all symbols are processed
        @type all_symbols: C{bool}
        @param symbols: list of symbols to process
        """
        data_dict_list = []
        if self.master_file == False: # no XMASTER file, return empty list
            return data_dict_list
        
        for stock in self.stocks:
            if all_symbols or (stock.stock_symbol in symbols):
                data_dict_list += stock.convert2list(dir_path)
                
        return data_dict_list
