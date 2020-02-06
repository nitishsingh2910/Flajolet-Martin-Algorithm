# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 01:08:15 2019

@author: nitis
"""

import numpy as np
import gzip
import mmh3
from statistics import mean, median
from multiprocessing import Pool
# =============================================================================
# Global variables
# =============================================================================
NUM_OF_HASH_FUNCTIONS = 10
MAX_TAIL_LENGTH_LIST = [0]*NUM_OF_HASH_FUNCTIONS
SIZE_GROUP = 5
FILENAMES = ["quotes_2008-08.txt.gz", "quotes_2008-09.txt.gz", "quotes_2008-10.txt.gz", 
             "quotes_2008-11.txt.gz", "quotes_2008-12.txt.gz", "quotes_2009-01.txt.gz",
             "quotes_2009-02.txt.gz", "quotes_2009-03.txt.gz", "quotes_2009-04.txt.gz"]

def get_tail_length(bitArr):
    """ Calculates the number of 0s at the end of the bitstring """
    bitStr = str(bitArr)
    return len(bitArr) - len(bitStr.rstrip('0'))

def process_line(line):
    """ takes a string, applies 10 hash functions on it 
    returns a list of tail lengths for each hash function """
# =============================================================================
# get the hash values for each fxn and convert it to bit string
# =============================================================================
    binaryHashValues = [format(mmh3.hash(line, seed=i, signed=False), '032b') for i in range(0,NUM_OF_HASH_FUNCTIONS)]
# =============================================================================
# get the tail length for each hash fxn
# =============================================================================
    tailLengths = [get_tail_length(val) for val in binaryHashValues]
    return tailLengths

def process_one_file(filename):
    """ Reads a zipped file and returns the maix tail lenth for each hash function """
    with gzip.open(filename,"rb") as file:
        fileTailLengths = [0]*NUM_OF_HASH_FUNCTIONS
        for line in file:  
            if chr(line[0])=='Q':
                #get the tail length for each hash function
                tailLengths = process_line(line[2:])
                #get the maximum tail length for each hash function
                for i in range(0,NUM_OF_HASH_FUNCTIONS):
                    fileTailLengths[i] = max(fileTailLengths[i], tailLengths[i])
        return fileTailLengths
    
def main():
    a = 0
    for file in FILENAMES:
        fileTailLengths = process_one_file(file)
        for i in range(0,NUM_OF_HASH_FUNCTIONS):
            MAX_TAIL_LENGTH_LIST[i] = max(MAX_TAIL_LENGTH_LIST[i], fileTailLengths[i])
        print("File Completed ", a)
        a += 1
    R = mean([median(MAX_TAIL_LENGTH_LIST[i:i+SIZE_GROUP]) for i in range(0,NUM_OF_HASH_FUNCTIONS, SIZE_GROUP)])
    
    print("R is ", R)
    print("Unique count is ", 2**R)
    
    
if __name__ == '__main__':
# =============================================================================
#     pool = Pool(4)
#     reult = pool.map(process_one_file, FILENAMES)
# =============================================================================
    main()
                        
                
                
                