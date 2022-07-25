#!/usr/bin/env python3
# coding: utf8
# Written by Galchenkova M., 2021

import os
import subprocess
import sys
import re
import numpy as np
import pandas as pd
import glob
import logging
import argparse
import concurrent.futures

x_arg_name = 'd'
y_arg_name = 'CC*'
y_arg_name2 = 'Rsplit/%'

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


def get_xy(file_name, x_arg_name, y_arg_name):
    x = []
    y = []

    with open(file_name, 'r') as stream:
        for line in stream:
            if y_arg_name in line:
                tmp = line.replace('1/nm', '').replace('# ', '').replace('centre', '').replace('/ A', '').replace(' dev','').replace('(A)','')
                tmp = tmp.split()
                y_index = tmp.index(y_arg_name)
                x_index = tmp.index(x_arg_name)

            else:
                tmp = line.split()
                
                x.append(float(tmp[x_index]) if not np.isnan(float(tmp[x_index])) else 0. )
                y.append(float(tmp[y_index]) if not np.isnan(float(tmp[y_index])) else 0. )

    x = np.array(x)
    y = np.array(y)

    list_of_tuples = list(zip(x, y))
    
    df = pd.DataFrame(list_of_tuples, 
                  columns = [x_arg_name, y_arg_name])
    
    df = df[df[y_arg_name].notna()]
    df = df[df[y_arg_name] >= 0.]
    return df[x_arg_name], df[y_arg_name]

def calculating_max_res_from_Rsplit_CCstar_dat(CCstar_dat_file, Rsplit_dat_file):
    d_CCstar, CCstar = get_xy(CCstar_dat_file, x_arg_name, y_arg_name)
    CCstar *= 100
    
    d_Rsplit, Rsplit = get_xy(Rsplit_dat_file, x_arg_name, y_arg_name2)
    
    i = 0

    CC2, d2 = CCstar[0], d_CCstar[0]
    CC1, d1 = 0., 0.

    Rsplit2 = Rsplit[0]
    Rsplit1 = 0.

    while Rsplit[i]<=CCstar[i] and i < len(d_CCstar):
        CC1, d1, Rsplit1 = CC2, d2, Rsplit2
        i+=1
        try:
            CC2, d2, Rsplit2 = CCstar[i], d_CCstar[i], Rsplit[i]
        except:
            return -1000            
        if Rsplit[i]==CCstar[i]:
            resolution = d_CCstar[i]
            return resolution
            
    k1 = round((CC2-CC1)/(d2-d1),3)
    b1 = round((CC1*d2-CC2*d1)/(d2-d1),3)     

    k2 = round((Rsplit2-Rsplit1)/(d2-d1),3)
    b2 = round((Rsplit1*d2-Rsplit2*d1)/(d2-d1),3)

    #resolution = round(0.9*(b2-b1)/(k1-k2),3) 
    resolution = round(0.98*(b2-b1)/(k1-k2),3)
    return resolution

def parsing_stream(stream):
    global input_path
    
    folder = stream.split('.')[0]
    
    #CCstar_dat_file = glob.glob(f'{folder}*CCstar.dat', recursive=False)[0] if len(glob.glob(f'{folder}*CCstar.dat', recursive=False)) > 0 else ''#CCstar dat file for estimation max res for hits finding
    #Rsplit_dat_file = glob.glob(f'{folder}*Rsplit.dat', recursive=False)[0] if len(glob.glob(f'{folder}*Rsplit.dat', recursive=False)) > 0 else ''#Rsplit dat file for estimation max res for hits finding                                   
    
    CCstar_dat_files = glob.glob(f'{folder}*CCstar.dat', recursive=False) if len(glob.glob(f'{folder}*CCstar.dat', recursive=False)) > 0 else ''
    print_line = ''
    
    for CCstar_dat_file in CCstar_dat_files:
        Rsplit_dat_file = CCstar_dat_file.replace('CCstar','Rsplit')
        data_resolution = calculating_max_res_from_Rsplit_CCstar_dat(CCstar_dat_file, Rsplit_dat_file) if len(CCstar_dat_file) > 0 and len(Rsplit_dat_file) > 0 else -1000
        
        try:
            res_hits = subprocess.check_output(['grep', '-rc', 'hit = 1', stream]).decode('utf-8').strip().split('\n')
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
        
        name_of_test = os.path.basename(CCstar_dat_file).split('.')[0].replace('_CCstar','')
        print(name_of_test, data_resolution)
        #print_line += f'{os.path.basename(stream).replace(".stream",""):<10}; {str(chunks)+"/"+str(hits):^10}; {str(indexed_patterns)+"/"+str(indexed):^10}; {str(data_resolution):^10}\n'
        print_line += f'{name_of_test:<20}; {str(chunks)+"/"+str(hits):^10}; {str(indexed_patterns)+"/"+str(indexed):^10}; {str(data_resolution):^10}\n'
    return print_line

if __name__ == "__main__":

    args = parse_cmdline_args()
    input_path = args.path_from
    sample = args.s

    folders = []

    level = logging.INFO
    logger = logging.getLogger('app')
    logger.setLevel(level)
    log_file = 'statistics-v7.log'
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    f_handler = logging.FileHandler(os.path.join(os.getcwd(), log_file))
    logger.addHandler(f_handler)
    logger.info(f'{"stream":<20};{"num patterns/hits":^10};{"indexed patterns/indexed crystals":^10};;{"CC* intersects with Rsplit at":^10}')
    print("Log file is {}".format(os.path.join(os.getcwd(), log_file)))
    
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
    
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = [ executor.submit(parsing_stream, folder) for folder in folders]
        
        for f in concurrent.futures.as_completed(results):
            #print(f.result())
            logger.info(f.result())
    print('Finished')
