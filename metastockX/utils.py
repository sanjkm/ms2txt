"""
Helper methods
"""

import struct
import datetime

def fmsbin2ieee(bytes):
    """
    Convert an array of 4 bytes containing Microsoft Binary floating point
    number to IEEE floating point format (which is used by Python)
    """
    as_int = struct.unpack("i", bytes)
    if not as_int:
        return 0.0
    man = long(struct.unpack('H', bytes[2:])[0])
    if not man:
        return 0.0
    exp = (man & 0xff00) - 0x0200
    man = man & 0x7f | (man << 8) & 0x8000
    man |= exp >> 1

    bytes2 = bytes[:2]
    bytes2 += chr(man & 255)
    bytes2 += chr((man >> 8) & 255)
    return struct.unpack("f", bytes2)[0]

def float2date(date):
    """
    Metastock stores date as a float number.
    Here we convert it to a python datetime.date object.
    """
    date = int(date)
    year = 1900 + (date / 10000)
    month = (date % 10000) / 100
    day = date % 100
    return datetime.date(year, month, day)

def float2time(time):
    """
    Metastock stores date as a float number.
    Here we convert it to a python datetime.time object.
    """
    time = int(time)
    hour = time / 10000
    minute = (time % 10000) / 100
    return datetime.time(hour, minute)

def convertSymbolName (sym_name):
    """
    Gets rid of the first two symbols of the symbol name, as well as
    all the characters after (and including) the '#' symbol
    """
    beg_chars = 2
    
    pound_char = "#"
    pound_index = sym_name.find(pound_char)
    
    return sym_name[beg_chars:pound_index]
