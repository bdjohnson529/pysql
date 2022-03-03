#!/usr/bin/env python
# encoding: utf-8
"""
BambooLib - Hashing

Hashing functions

author : Ben Johnson
"""

def hash_string(input_str):
    """
    Computes the md5 hash of a string.
    The md5 hashing function generates a hexadecimal number.
    Only the final 16 digits of the hex number are converted to a decimal number.
    This will result in some collisions.
    
    :param input_str: Input string
    :type input_str: str
    """
    encoded_str = input_str.encode('utf-8')
    result = hashlib.md5(encoded_str)
    hex_num = result.hexdigest().upper()
    decimal_num = int(hex_num[16:], 16)    
    
    return decimal_num
