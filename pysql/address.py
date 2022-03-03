"""
Address.py
====================================
Address parsing module
"""

import pandas as pd
import numpy as np
import scourgify

def parseStreetAddress(inputStr):
    """
    Parse a street address.

    :param inputStr: Full address string
    :type inputStr: str
    """
    street = None
    city = None
    state = None
    zip5 = None
    zip9 = None
    addrDict = None
    
    if not inputStr:
        return pd.Series(['','','','',''], index=['Street', 'City', 'State', 'Zip5', 'Zip9'])
    
    try:
        addrDict = scourgify.normalize_address_record(inputStr)
    except:
        pass
        
    if not addrDict:
        return pd.Series(['','','','',''], index=['Street', 'City', 'State', 'Zip5', 'Zip9'])
        
    # set service address
    if addrDict['address_line_2']:
        street = addrDict['address_line_1'] + " " + addrDict['address_line_2']
    else:
        street = addrDict['address_line_1']
    
    # set service city
    if addrDict['city']:
        city = addrDict['city']

    # set service state
    if addrDict['state']:
        state = addrDict['state']

    # set service zip
    if addrDict['state']:
        postalCode = formatPostalCode(addrDict['postal_code'])
        
        zip5 = postalCode[0]
        zip9 = postalCode[1]
    
    
    return pd.Series([street, city, state, zip5, zip9], index=['Street', 'City', 'State', 'Zip5', 'Zip9'])


def formatPostalCode(inputCode):
    """
    Format postal code

    :param inputCode: 9 or 5 digit postal code
    :type inputCode: str
    """
    codeSplit = inputCode.split('-')
    
    if(len(codeSplit) > 1):
        return (codeSplit[0], inputCode)
    else:
        return (codeSplit[0], None)