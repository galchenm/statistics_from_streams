#!/usr/bin/env python3
# coding: utf8
# Written by Galchenkova M., 2021

import os
import subprocess
import sys
import re
import numpy as np
import glob
import logging
import argparse
import concurrent.futures

class CustomFormatter(argparse.RawDescriptionHelpFormatter,
                      argparse.ArgumentDefaultsHelpFormatter):
    pass

def parse_cmdline_args():
    parser = argparse.ArgumentParser(
        description=sys.modules[__name__].__doc__,
        formatter_class=CustomFormatter)
    parser.add_argument('path_from', type=str, help="The path of folder/s that contain/s files")
    
    parser.add_argument('-p','--p', type=str, help="Pattern in name")
    parser.add_argument('-ap','--ap', type=str, help="Additional pattern in name")
    parser.add_argument('-s','--s', type=str, help="Sample in name")
    parser.add_argument('-f','--f', type=str, help='File with blocks')
    return parser.parse_args()

def parsing_stream(stream):
    global input_path
    
    try:
        res_hits = subprocess.check_output(['grep', '-rc', 'hit = 1',stream]).decode('utf-8').strip().split('\n')
        hits = int(res_hits[0])
    except subprocess.CalledProcessError:
        hits = 0


    try:
        chunks = int(subprocess.check_output(['grep', '-c', 'Image filename',stream]).decode('utf-8').strip().split('\n')[0]) #len(res_hits)
    except subprocess.CalledProcessError:
        chunks = 0

    try:
        res_indexed = subprocess.check_output(['grep', '-rc', 'Begin crystal',stream]).decode('utf-8').strip().split('\n')
        indexed = int(res_indexed[0])
    except subprocess.CalledProcessError:
        indexed = 0

    try:
        res_none_indexed_patterns = subprocess.check_output(['grep', '-rc', 'indexed_by = none',stream]).decode('utf-8').strip().split('\n')
        none_indexed_patterns = int(res_none_indexed_patterns[0])
    except subprocess.CalledProcessError:
        none_indexed_patterns = 0


    indexed_patterns = chunks - none_indexed_patterns
    
    #print_line = f'{os.path.basename(stream):<10}; num patterns/hits = {str(chunks)+"/"+str(hits):^10}; indexed patterns/indexed crystals = {str(indexed_patterns)+"/"+str(indexed):^10}'
    print_line = f'{os.path.basename(stream).replace(".stream",""):<10}; {str(chunks)+"/"+str(hits):^10}; {str(indexed_patterns)+"/"+str(indexed):^10}'
    return print_line

if __name__ == "__main__":

    args = parse_cmdline_args()
    input_path = args.path_from
    sample = args.s

    folders = []

    level = logging.INFO
    logger = logging.getLogger('app')
    logger.setLevel(level)
    log_file = 'statistics.log'
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    f_handler = logging.FileHandler(os.path.join(os.getcwd(), log_file))
    logger.addHandler(f_handler)
    logger.info(f'{"stream":<10};{"num patterns/hits":^10};{"indexed patterns/indexed crystals":^10};')
    print("Log file is {}".format(os.path.join(os.getcwd(), log_file)))
    
    '''
    for path, dirs, all_files in os.walk(input_path):
        streams = glob.glob(os.path.join(path, '*.stream'))
        if len(streams) != 0:
            folders += streams
    '''
    if args.f is None:
        ffolders = glob.glob(os.path.join(input_path,'*/j_stream/*.stream')) + glob.glob(os.path.join(input_path,'*/j_stream/prev/*.stream')) + glob.glob(os.path.join(input_path,'*.stream'))
        
    else:
        ffolders = []
        with open(args.f,'r') as f:
            for d in f:
                d = d.strip()
                
                for path, dirs, all_files in os.walk(input_path):
                    for di in dirs:
                        
                        if d == di:
                            print(di)
                            current_path = os.path.join(path, di)
                            s = glob.glob(os.path.join(current_path, 'j_stream/*.stream')) + glob.glob(os.path.join(current_path,'*/j_stream/prev/*.stream'))
                            ffolders += s
    
    if args.s is not None:
        folders = [stream for stream in ffolders if args.s in os.path.basename(stream).split('.')[0]]

    elif args.p is not None:
        folders = [stream for stream in ffolders if args.p in os.path.basename(stream).split('.')[0]]
    
    elif args.ap is not None:
        folders = [stream for stream in ffolders if args.ap in os.path.basename(stream).split('.')[0]]
    else:
        folders = ffolders
        
    folders.sort()
    print(len(folders))
    
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = [ executor.submit(parsing_stream, folder) for folder in folders]
        
        for f in concurrent.futures.as_completed(results):
            #print(f.result())
            logger.info(f.result())
    print('Finished')
