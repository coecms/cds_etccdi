#!/usr/bin/python
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
# This file contains functions called by the main cli.py program
# contact: paola.petrelli@utas.edu.au
# last updated 14/06/2022

import logging
import json
import os
import re
import yaml
import pkg_resources
import subprocess as sp
from calendar import monthrange
from datetime import datetime
from itertools import repeat
from concurrent.futures import ThreadPoolExecutor
import cds.cds_db as cds_db
import cds.cdsapi as cdsapi


def config_log(debug):
    ''' configure log file to keep track of users queries '''
    # start a logger
    logger = logging.getLogger('cdslog')
    # set a formatter to manage the output format of our handler
    formatter = logging.Formatter('%(levelname)s %(asctime)s; %(message)s',"%Y-%m-%d %H:%M:%S")
    # set cdslog level explicitly otherwise it inherits the root logger level:WARNING
    # if debug: level DEBUG otherwise INFO
    # because this is the logger level it will determine the lowest possible level for thehandlers
    if debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logger.setLevel(level)
    
    # add a handler to send WARNING level messages to console
    clog = logging.StreamHandler()
    if debug:
        level = logging.DEBUG
    else:
        level = logging.WARNING
    clog.setLevel(level)
    logger.addHandler(clog)    

    # add a handler to send INFO level messages to file 
    # the messages will be appended to the same file
    # create a new log file every day 
    date = datetime.now().strftime("%Y%m%d") 
    logname = cfg['logdir'] + '/etccdi_log_' + date + '.txt' 
    flog = logging.FileHandler(logname) 
    try:
        os.chmod(logname, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO);
    except:
        pass 
    flog.setLevel(logging.INFO)
    flog.setFormatter(formatter)
    logger.addHandler(flog)

    # return the logger object
    return logger


def read_json(fname):
    """
    Read data from json file
    """
    try:
        jfile = pkg_resources.resource_filename(__name__, f'data/{fname}')
        with open(jfile,'r') as fj:
            data = json.load(fj)
    except FileNotFoundError:
        print(f"Can't find file {fname} in {os.getcwd()}/data")
        raise SystemExit() 
    return data


def define_var(vardict, varparam, cdslog):
    """ Find grib code in vardict dictionary and return relevant info
    """
    queue = True
    try:
        name, cds_name = vardict[varparam]
    except:
       cdslog.info(f'Selected parameter code {varparam} is not available')
       queue = False
       return queue, None, None
    return queue, name, cds_name


def define_args(index, tstep):
    ''' Return parameters and levels lists and step, time depending on index type'''
    # this import the index_dict dictionary <index> : ['time','step','params','levels']
    # I'm getting the information which is common to all pressure/surface/wave variables from here, plus a list of the variables we download for each index 
    index_file = pkg_resources.resource_filename(__name__, f'data/{index}_{tstep}.json')
    with open(index_file, 'r') as fj:
        dsargs = json.load(fj)
    return  dsargs


def read_vars(index):
    """Read parameters info from <index>_vars.json file
    """
    var_file = pkg_resources.resource_filename(__name__, f'data/{index}_vars.json')
    with open(var_file,'r') as fj:
         vardict = json.load(fj)
    return vardict 


def file_exists(fn, nclist):
    """ check if file already exists
    """
    return fn in nclist 


def build_dict(dsargs, prod, exp, mod, params, oformat):
    """Builds request dictionary to pass to retrieve command 
    """
    period = dsargs[f'period_{exp[:3]}']
    rdict={ 'format'      : oformat,
            'variable'    : params,
            'product_type': prod,
            'model'       : mod,
            'experiment'  : exp,
            'period'      : period 
          }

    for k in ['ensemble_member', 'temporal_aggregation', 'version']:
        rdict[k] = dsargs[k]
    return rdict 


def file_down(url, tempfn, size, cdslog):
    """ Open process to download file
        If fails try tor esume at least once
        :return: success: true or false
    """
    cmd = f"{cfg['getcmd']} {tempfn} {url}"
    cdslog.info(f"CDS Downloading: {url} to {tempfn}")
    p = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
    out,err = p.communicate()
    # alternative check using actual file size
    # limited to number of retry set in config.json
    n = 0
    if os.path.getsize(tempfn) == size:
        return True
    while os.path.getsize(tempfn) < size and n < cfg['retry']:
        cmd = f"{cfg['resumecmd']} {tempfn} {url}"
        cdslog.info(f'CDS Resuming download {n+1}: {url} to {tempfn}')
        p1 = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        out,err = p1.communicate()
        # need to add something to break cycle if file unavailable or at least limits reruns
        if not p1.returncode:            # successful
            return True
        #elif "unavailable":
            #return False
        else:
            n+=1
    # if there's always a returncode for all retry return false
    return False
    

def target(index, prod, exp, model, tstep, params, oformat):
    """Build output paths and filename, 
       build list of days to process based on year and month
       This should become a generic output for tar/zip and destdir based on model??
       probably not we don't want to prolifirate request keep request to prod+exp+tstep
    """
    # set output paths and zip/tar file name
    # map model name to same as filename
    mapping = read_json('model_map.json')
    stagedir = os.path.join(cfg['staging'],index, prod, tstep, exp, mapping[model])
    destdir = os.path.join(cfg['datadir'],index, prod, tstep, exp, mapping[model])
    zipname = f"{index}_{prod}_{tstep}_{exp}_{model}.{oformat}"
    # create list of files which will be in zip/tar file
    var_dict = read_vars(index)
    prod = expand_prod(prod, reverse=True)
    flist = []
    for var in params:
        vname = var_dict[var] 
        flist.append(f"{vname}{index.upper()}_{tstep}_{model}_{exp}_*_{prod}_*_*_v1-0.nc")
    return stagedir, destdir, zipname, flist


def dump_args(args, urgent):
    """ Create arguments dictionary and dump to json file
    """
    tstamp = datetime.now().strftime("%Y%m%d%H%M%S") 
    fname = f'cds_request_{tstamp}.json'
    requestdir = cfg['requestdir'] 
    if urgent:
        requestdir += 'Urgent/'
    with open(requestdir + fname, 'w+') as fj:
         json.dump(args, fj)
    return

def process_files(conn, index, prod, tstep, exp, mod, params, oformat, cdslog):
    """
    # create list of filenames already existing for this model
    temporarily pass alos params to be quicker we're downloading a 
    model at the time all params but theoritically we should put in more sophisticated checks 
    """
    flist = []
    sql = "select filename from file where location=?"
    tup = (f"{index}/{prod}/{tstep}/{exp}/{mod}",)
    flist += cds_db.query(conn, sql, tup)
    cdslog.debug(flist)
    stagedir, destdir, fname, ziplist = target(index, prod,
            exp, mod, tstep, params, oformat)
    # if file already exists in datadir then skip
    # eventually introduce a way to switch off checks
    cdslog.debug(f"Stagedir: {stagedir}")
    cdslog.debug(f"Destdir: {destdir}")
    cdslog.debug(f"Archive filename: {fname}")
    cdslog.debug(f"List of fname regex: {ziplist}")
    skip_files = [j for j in ziplist for i in flist if re.match(i,j)]
    cdslog.debug(f"List of files to skip: {skip_files}")
    return skip_files, stagedir, destdir, fname


def expand_prod(prod, reverse=False):
    """
    """
    prod_dict = {'bias_adj': 'bias_adjusted',
                 'raw': 'non_bias_adjusted',
                 'b1961_1990': 'base_period_1961_1990',
                 'b1981_2010': 'base_period_1981_2010',
                 'no-base': 'base_independent'}
    if reverse:
        pdict = dict((v, k) for k, v in prod_dict.items())
    else:
        pdict = prod_dict

    if type(prod)==str:
        prod = [prod]
    new_prod = [pdict[x] for x in prod]
    return new_prod


def do_request(r, cdslog):
    """
    Issue the download request. param 'r' is a tuple:
    [0] dataset name
    [1] the query
    [2] file staging path
    [3] file target path
    [4] ip for download url
    [5] userid

    Download to staging area first, compress netcdf (nccopy)
    """
    tempfn = r[2]
    fn = r[3]
  
    # the actual retrieve part
    # set api key explicitly so you can alternate
    try:
        with open(f'/mnt/pvol/etccdi/.cdsapirc{r[5]}', 'r') as f:
            credentials = yaml.safe_load(f)
    except:
        print("issue opening credentials!")
    cdslog.debug(f"Credentials {credentials} ... ")
    # create client instance
    c = cdsapi.Client(url=credentials['url'], key=credentials['key'], verify=1)
    cdslog.debug(f"Client {c} ... ")

    cdslog.info(f"Requesting {tempfn} ... ")
    cdslog.info(f"Request: {r[1]}")
    # need to capture exceptions
    apirc = False
    try:
        # issue the request (to modified api)
        cdslog.debug(r[0], r[1])
        res = c.retrieve(r[0], r[1], tempfn)
        apirc = True
    except Exception as e:
        cdslog.error(f'ERROR: {e}')
        apirc = False
    cdslog.debug(f"apirc: {apirc}")
    # if request successful download file
    if apirc:
        # get download url and replace ip
        url = res.location
        cdslog.debug(f"Request url: {url}")
        # get size from response to check file complete
        size = res.content_length
        # check for slow IPs
        for ip in cfg['slowips']:
            if f'.{ip}/' in res.location:
                url = res.location.replace(f'.{ip}/', f'.{r[4]}/')
        if file_down(url, tempfn, size, cdslog):            # successful
            # if netcdf compress file, assuming it'll fail if file is corrupted
            # if tgz  untar, if zip unzip
            # if grib skip
            if tempfn[-3:] == '.nc':
                cdslog.info(f"Compressing {tempfn} ...")
                cmd = f"{cfg['nccmd']} {tempfn} {fn}"
            elif tempfn[-4:] == '.tgz':
                cdslog.info(f"Untarring {tempfn} ...")
                #base = os.path.dirname(tempfn) 
                base = r[3] 
                cmd =  f"{cfg['untar']} {tempfn} -C {base}"
            elif tempfn[-4:] == '.zip':
                cdslog.info(f"Unzipping {tempfn} ...")
                base = r[3] 
                #base = os.path.dirname(tempfn) 
                cmd =  f"unzip {tempfn} -d {base}"
            else:
                cmd = "echo 'nothing to do'"
            cdslog.debug(f"{cmd}")
            p = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
            out,err = p.communicate()
            cdslog.debug(f"Popen out/err: {out}, {err}")
            if not p.returncode:       # check was successful
                cdslog.info(f"CDS download success: {fn}")
            else:
                cdslog.info("CDS nc command failed! (deleting "+
                            f" compressed file {fn})\n{err.decode()}")
    return


def api_request(ctx, args, scan=False):
    """ Build a list of CDSapi requests based on arguments
        Call do_request to submit them and start parallel download
        If download successful, unzip/tar and move to dataset directory 
    """
    # open connection to etccdi files db 
    conn = cds_db.db_connect(cfg)
    cdslog = ctx.obj['log']
    # create empty list to  store cdsapi requests
    rqlist = []
    # list of faster ips to alternate
    ips = cfg['altips']
    users = cfg['users']
    i = 0 

    # retrieve index arguments
    if scan:
        index = args['index']
        tstep = args['tstep']
    else:
        index = ctx.obj['index']
        tstep = ctx.obj['tstep']
    cdslog.debug(f'Index: {index}')
    cdslog.debug(f'Timestep: {tstep}')
    dsargs = ctx.obj['dsargs']
    cdslog.debug(f'Index attributes: {dsargs}')

    # define other params and models if they're not listed in args
    params = args['params']
    if params == []:
        params = dsargs['variable']
    models = args['model']
    if models == []:
        models = dsargs['model']
    exps = args['experiment']
    if exps == []:
        exps = dsargs['experiment']

    cdslog.debug(f'Params: {params}')
    
    # Loop through products and experiments and do either multiple
    # variables in one request, or at least loop through variables in the innermost loop.
    
    for pt in args['prod']:
        cdslog.debug(f"Product type: {pt}")
        for exp in exps:
            cdslog.debug(f'Experiment: {exp}')
            # for each output file build request and append to list
            # loop through params and modes requested
            for mod in models:
                cdslog.debug(f'Model: {mod}')
                # if file already exists in datadir then skip
                # eventually introduce a way to switch off checks
                skip_files, stagedir, destdir, fname = process_files(conn,
                    index, pt, tstep, exp, mod, params, args['format'], cdslog)  
                if skip_files != []:
                    cdslog.info(f'Skipping {fname} some files already exist')
                    continue
                # create path if required
                if not os.path.exists(stagedir):
                    os.makedirs(stagedir)
                if not os.path.exists(destdir):
                    os.makedirs(destdir)
                rdict = build_dict(dsargs, pt, exp, mod, params, args['format'])
                rqlist.append((dsargs['dsid'], rdict, os.path.join(stagedir,fname),
                           destdir, ips[i % len(ips)],
                           users[i % len(users)])) 
                # progress index to alternate between ips and users
                i+=1
                cdslog.info(f"Added request for {fname}")
    
    cdslog.debug(f"Request list:\n {rqlist}")
    # parallel downloads
    if len(rqlist) > 0:
        cdslog.debug(f"Requests number: {len(rqlist)}")
        cdslog.info("Submitting requests")
        # set num of threads = number of models, or use default from config
        #if len(models) >= cfg['nthreads']:
        nthreads = cfg['nthreads']
        #else:
        #    nthreads = len(models)
        executor = ThreadPoolExecutor(max_workers=cfg['nthreads'])
        results = executor.map(do_request, rqlist, repeat(cdslog))
        executor.shutdown()
        #pool = ThreadPool(nthreads)
        #results = pool.imap(do_request, rqlist)
        #pool.close()
        #pool.join()
    else:
        cdslog.info('No files to download!')
    cdslog.info('--- Done ---')


cfg = read_json('config.json')
