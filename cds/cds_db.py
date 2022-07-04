#!/usr/bin/env python
# Copyright 2022 ARC Centre of Excellence for Climate Extremes (CLEX)
# Author: Paola Petrelli <paola.petrelli@utas.edu.au> for CLEX 
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Crawl etccdi netcdf directories and update ETCCDI db with new files found
# contact: paola.petrelli@utas.edu.au
# last updated 27/04/2022

import os
import sqlite3
import re
import csv
from datetime import datetime
from glob import glob
from itertools import repeat
from calendar import monthrange 
import cds.cds_functions as cdsf


def db_connect(cfg):
    """ connect to ETCCDI files sqlite db
    """
    return sqlite3.connect(cfg['db'], timeout=10, isolation_level=None)


def create_table(conn):
    """ create file table if database doesn't exists or empty
        :conn  connection object
    """
    file_sql = ("CREATE TABLE IF NOT EXISTS file( filename TEXT PRIMARY KEY,"+
                "location TEXT, ncidate TEXT, size INT, index_type TEXT, " +
                "product TEXT, timestep TEXT, experiment TEXT, model TEXT," +
                " ensemble TEXT, variable TEXT);") 
    try:
        c = conn.cursor()
        c.execute(file_sql)
    except Error as e:
        print(e)


def query(conn, sql, tup, first=True):
    """ generic query
    """
    with conn:
        c = conn.cursor()
        c.execute(sql, tup)
        if first:
            result = [ x[0] for x in c.fetchall() ]
        else:
            result = [ x for x in c.fetchall() ]
        return result 

def get_attributes(f, dn, fn, basedir):
    """
    """
    tsfmt = '%FT%T'
    attrs = [fn] 
    s = os.stat(f)
    location =  dn.replace(basedir + '/', '')
    attrs.append( location )
    attrs.append( datetime.fromtimestamp(s.st_mtime).strftime(tsfmt) )
    attrs.append( s.st_size )
    #idx, prod, tstep, exp, model = location.split("/")
    attrs.extend( location.split('/') )
    attrs.append( fn.split('_')[4] )
    idx = attrs[4]
    if idx == 'etccdi':
        var = fn.split('ETCCDI')[0]
    else:
        var = fn.split('_')[0]
    attrs.append( var )
    return tuple(attrs)


def crawl(g, xl, basedir):
    """ Crawl base directory for all netcdf files
    """
    # get stats for all files not in db yet
    file_list = []
    for f in g:
        dn, fn = os.path.split(f)
        if not fn in xl:
            # get attributes
            attrs = get_attributes(f, dn, fn, basedir)
            file_list.append( attrs )
    return file_list


def set_query(idx, prod, tstep, exp='%',model='%'):
    """ Set up the sql query based on constraints
    """
    # name: tn90pETCCDI_mon_BCC-CSM2-MR_ssp585_r1i1p1f1_b1981-2010_v20191108_201501-210012_v1-0.nc 
    # expand_prod returns list even if here we always pass only 1 product
    if prod:
        pr = cdsf.expand_prod(prod, reverse=True)
        pr = pr[0].replace("_","-")
    else:
        pr = "%"
    if idx in ['etccdi','hsi']:
        fname = f'%{idx.upper()}_{tstep}_%_{exp}_%_{pr}_%_v1-0.nc'
        location = f'{idx}/{prod}/{tstep}/{exp}/{model}'
    else:
        fname = f'%_{idx.upper()}_{tstep}_{model}_{exp}_%.nc'
        location = f'{idx}/{prod}/{tstep}/{exp}/{model}'
    return fname, location


def list_files(basedir, match):
    """ List all matching files for given base dir, index and tstep
    """
    fsmatch = match.replace("%", "*")
    d = os.path.join(basedir, fsmatch, "*.nc")
    print(f'Searching on filesystem: {d} ...')
    g = glob(d)
    print(f'Found {len(g)} files.')
    return g 


def update_db(cfg, index, tstep, prod, exps, models):
    # read configuration and open ETCCDI files database
    conn = db_connect(cfg)
    create_table(conn)

    # List all netcdf files in datadir
    if not index:
        sql = 'SELECT filename FROM FILE ORDER BY filename ASC'
        fs_list = list_files(cfg['datadir'],'*/*/*/*/*')
    else:
        fs_list = []
        if not exps:
            exps=['%']
        if not models:
            models=['%']
        for exp in exps:
            for mod in models:
                fname, location = set_query(index, prod, tstep, exp, mod)
                basedir = cfg['datadir']
            sql = f"SELECT filename FROM file AS t WHERE t.location LIKE '{location}' ORDER BY filename ASC"
            fs_list.extend(list_files(basedir, location))

    xl = query(conn, sql, ())
    if not index:
        stats_list = crawl(fs_list, xl, cfg['datadir'])
    else:
        stats_list = crawl(fs_list, xl, basedir)
    print(f'Records already in db: {len(xl)}')
    print(f'New files found: {len(fs_list)-len(xl)}')
    # insert into db
    if len(stats_list) > 0:
        print('Updating db ...')
        with conn:
            c = conn.cursor()
            sql = ("INSERT OR IGNORE INTO file (filename, location, ncidate," +
                  " size, index_type, product, timestep, experiment, model, " +
                "ensemble, variable) values (?,?,?,?,?,?,?,?,?,?,?)" )
            #debug(sql)
            c.executemany(sql, stats_list)
            c.execute('select total_changes()')
            print('Rows modified:', c.fetchall()[0][0])
    print('--- Done ---')


def models_stats(ctx, cfg, idx, tstep, prod, varlist=[], models=[], verbose=False):
    """
    """
    # read configuration and open ERA5 files database
    conn = db_connect(cfg)

    for pr in prod:
        mods, variables = list_mod_var(idx, tstep, pr, ctx.obj['dsargs'])
        if models == []:
            models = mods
        print(f"\nIndexes for {pr} product")
        # get the total of expected files based on the number of variables
        # 5 experiments * ensemble-number (the last is unknown
        total = len(variables) * 5
        print(f'There should be {total} files for each model ensemble\n')
        for mod in models:
            print(f"  Model {mod}:")
            fname, location = set_query(idx, pr, tstep, exp='%',model=mod)
            matches = get_matches(fname, idx, mod, variables)
            sql = (f"SELECT filename FROM file AS t WHERE "+
                  f"t.location LIKE '{location}' ORDER BY filename ASC")
            xl = query(conn, sql, ())
            print(f'  Indexes already in db: {len(xl)}')
            # Compare list of matches to each file to see which one aren't matched
            not_found = []
            for m in matches:
                r=re.compile(m)
                if not any(r.match(f) for f in xl):
                    not_found.append(m)
            print(f'  Indexes missing: {len(not_found)}')
            if verbose:
                print("  Files available\n")
                for f in sorted(xl):
                    print(f"    {f}")
                print()
                if len(not_found) > 0:
                    print("  Files missing\n")
                    for f in not_found:
                        print(f"    {f}")
                    print()


def list_mod_var(idx, tstep, prod, dsargs):
    """Get complete list of models and variables based on product"""
    if idx == 'hsi':
        prodargs = dsargs
    else:
        if prod == 'base_independent':
            pr = 'nobase'
        else:
            pr = 'base'
        prodargs = cdsf.define_args(idx, tstep + "_" + pr)
    return prodargs['model'], prodargs['variable']


def get_matches(pattern, idx, model, variables):
    """Get complete list of filename matches based on model and variables available"""
    # Pattern: f'%{idx.upper()}_{tstep}_%_{exp}_%_{pr}_%_%_v1-0.nc' 

    var_dict = cdsf.define_args(idx, 'vars')
    # got from sql wildcard to regex
    pattern = pattern.replace(".nc","\.nc").replace("%","(.*)")
    bits = pattern.split("_")
    files = []
    for var in variables:
        fname = "_".join([ bits[0].replace('(.*)',var_dict[var]),
            bits[1], model.replace("_","-").upper() ] + bits[3:])
        files.append(fname)
    return files
        

def delete_record(cfg, idx, prod, tstep, exp, mod):
    # connect to db
    conn = db_connect(cfg)
    #mn, yr, var = tuple(['%'] if not x else x for x in [mn, yr, var] ) 

    # Set up query
    for e in exp:
        for m in mod:
            fname, location = set_query(idx, prod, tstep, exp=e,model=m)
            sql = f'SELECT filename FROM file WHERE file.location="{location}"'
            print(sql)
            xl = query(conn, sql, ())
            print(f'Selected records in db: {xl}')
    # Delete from db
            if len(xl) > 0:
                confirm = input('Confirm deletion from database: Y/N   ')
                if confirm == 'Y':
                    print('Updating db ...')
                    for fname in xl:
                        with conn:
                            c = conn.cursor()
                            sql = f'DELETE from file where filename="{fname}" AND location="{location}"'
                            c.execute(sql)
                            c.execute('select total_changes()')
                            print('Rows modified:', c.fetchall()[0][0])
    return


def create_intake(cfg):
    """Get the entire table 'file' and save it as an intake csv file
    """
    conn = db_connect(cfg)
    # select relevant fields from the entire table 'file'
    sql=("SELECT filename, location, index_type, product, timestep, " +
         "experiment, model, ensemble, variable from file")
    file_list = query(conn, sql, (), first=False)
    # change accordingly!
    basedir = '/g/data/ia39/aus-ref-clim-data-nci/cmip6-etccdi/data/v1-0'

    # open csv file and write file_list 
    f = open('cmip6_etcddi.csv', 'w+', newline ='')
    header = ['path', 'index_type', 'base', 'frequency', 'experiment',
              'model', 'ensemble', 'variable', 'date_range']
    with f:
        write = csv.writer(f)
        write.writerow(header)
        for l in file_list:
            path = "/".join([basedir, l[1], l[0]])
            date_range = l[0].split("_")[-2] 
            newline = [path] + list(l[2:]) + [date_range]
            write.writerow(newline)
