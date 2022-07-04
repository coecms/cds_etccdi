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
# To download CMIP6 ETCCDI indexes from the Copernicus data server
# Download all the files for a year or selected months
# Area is global for surface, wave model and land variables variable while pressure levels are downloaded on
# Use: 

# depends on cdapi.py that can be downloaded from the Copernicus website
#   https://cds.climate.copernicus.eu/api-how-to
# contact: paola.petrelli@utas.edu.au
# last updated 04/07/2022

import click
import os
import sys
import yaml
from itertools import product as iproduct
from cds.cds_functions import *
from cds.cds_db import query, db_connect, update_db, delete_record, models_stats


@click.group()
@click.option('--debug', is_flag=True, default=False,
               help="Show debug info")
#@click.pass_context
def cds(debug):
    """
    Request and download data from the climate copernicus
    server using the cdsapi module.
    """
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    #ctx.ensure_object(dict)
    global cdslog
    cdslog = config_log(debug)


def download_args(f):
    '''Arguments to use with download sub-command '''

    # currently we get all possible values, even if choices might be different based on index type
    dsargs = define_args('all', 'values')
    constraints = [
        click.option('--index', '-i', required=True, default='etccdi',
              type=click.Choice(['etccdi','hsi']),
               help="Index type currently etccdi or hsi"),
        click.option('--tstep', '-t', required=True, default='yr',
              type=click.Choice(['yr','mon','day']),
              help="timestep yr, mon, for etccdi and day for hsi"),
        click.option('--product', '-pt', 'prod', required=True, multiple=True,
                     type=click.Choice(dsargs['product_type']),
                     help="Product type: if ETCCDI is the base period\n"+
                          "no-base, b1961_1990, b1981_2010\n"+
                          "If HIS define is: bias_adj, raw"
                     ),
        click.option('--model', '-m', required=False, multiple=True,
                     type=click.Choice(dsargs['model']), show_choices=True,help="CMIP6 model"
                    ),
        click.option('--experiment', '-e', required=False, multiple=True,
                     type=click.Choice(dsargs['experiment']), help="CMIP6 experiment: \n"+
                          "historical, ssp1_2_6, ssp2_4_5, ssp3_7_0, ssp5_8_5"
                     ),
        click.option('--param', '-p', multiple=True,
                     type=click.Choice(dsargs['variable']), show_choices=True,
             help="Selected variable. If not passed all variables in"+
                  "<index_tstep>.json will be downloaded"),
        click.option('--queue', '-q', is_flag=True, default=False,
                     help="Create json file to add request to queue"),
        click.option('--format', 'oformat', type=click.Choice(['zip','tgz']),
                     default='tgz',
                     help="Format output: tgz (compressed tar file - default) or zip"),
        click.option('--urgent', '-u', is_flag=True, default=False,
                     help="high priority request, default False, if specified request is saved in Urgent folder which is pick first by wrapper. Works only for queued requests.")
    ]
    for c in reversed(constraints):
        f = c(f)
    return f


def db_args(f):
    '''Arguments to use with db sub-command '''
    # currently we get all possible values, even if choices might be different based on index type
    dsargs = define_args('all', 'values')
    constraints = [
        click.option('--index', '-i', required=True, default='etccdi',
              type=click.Choice(['etccdi','hsi']),
               help="Index type currently etccdi or hsi"),
        click.option('--tstep', '-t', required=True, default='yr',
              type=click.Choice(['yr','mon','day']),
              help="timestep yr, mon, for etccdi and day for hsi"),
        click.option('--param', '-p', multiple=True,
            help="Variable name. At least one value is required in delete mode."+
            "If not passed in list mode all variables <index_tstep>.json will be listed"),
        click.option('--model', '-m', required=False, multiple=True,
            type=click.Choice(dsargs['model']), help="CMIP6 model. At "+
            "least one value is required in delete mode. If not passed in "+
            "list mode all models <index_tstep>.json will be listed"),
        click.option('--product', '-pt', 'prod', required=False, multiple=False,
            type=click.Choice(['product_type']),
            help="Base period (ETCCDI), bias_adjusted (HIS). At least one"
            " value is required in delete mode. If not passed in list mode "+
            "all products in <index_tstep>.json will be listed. In delete "+
            "mode pass only one value."),
        click.option('--experiment', '-e', required=False, multiple=True,
            type=click.Choice(dsargs['experiment']),
            help="CMIP6 experiment. At least one value is required in delete mode."+
                 "If not passed in list mode all experiments in "+
                 "<index_tstep>.json will be listed"),
        click.option('-a','--action', type=click.Choice(['list','delete',
            'update', 'intake']), default='update', help="db subcommand running mode:\n"+
            "`update` (default) updates the db,\n"+
            "`delete` deletes a record from db, `list` list all variables"+
            " in db for the index type\n"+
            " `intake` create an intake catalogue"),
        click.option('--verbose', '-v', is_flag=True, default=False,
            help="""To be used in conjuction with `list` action.
            It will print out a full list of missing or extra files,
            for every index type and timesteps where the total number of
            files on disk and the expected number of files do not match""")
    ]
    for c in reversed(constraints):
        f = c(f)
    return f


@cds.command()
@download_args
@click.pass_context
def download(ctx, index, tstep, oformat, param, prod, experiment,
             model, queue, urgent):
    """ 
    Download ETCCDI variables, to be preferred 
    if adding a new variable,
    if month argument is not passed 
    then the entire year will be downloaded. 
    By default downloads are in tgz format.
    
    Grid and other index type settings are in the <index>_<tstep>.json files.
    """
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)
    ctx.obj['log'] = cdslog
    valid_format = list(iproduct(['tgz','zip'],['etccdi','hsi']))
    if (oformat,index) not in valid_format:
        cdslog.info(f'Download format {oformat} not available for {index} product')
        sys.exit()
    valid_tstep = list(iproduct(['mon','yr'],['etccdi']))
    valid_tstep.append(('day','hsi'))
    prod = expand_prod(prod)
    if (tstep,index) not in valid_tstep:
        cdslog.info(f"Timestep {tstep} not available for {index} product")
        sys.exit()
    args = {'format':     oformat,
            'index':      index,
            'params':     list(param),
            'prod':       prod,
            'experiment': list(experiment),
            'model':      list(model),
            'tstep':      tstep}
    if queue:
        dump_args(args, urgent)
    else:    
        api_request(ctx, args) 


@cds.command()
@click.pass_context
@click.option('--file', '-f', 'infile', required=True,
             help="Pass json file with list of requests, instead of arguments")
def scan(ctx, infile):
    """ 
    Load arguments previously saved to file
    """
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)
    ctx.obj['log'] = cdslog
    with open(infile, 'r') as fj:
         args = json.load(fj)
    ctx.obj['dsargs'] = define_args(args['index'], args['tstep'])
    api_request(ctx, args, scan=True)


@cds.command()
@db_args
@click.pass_context
def db(ctx, index, tstep, param, model, experiment, prod, action, verbose):
    """ 
    Work on database, options are 
    - update database,
    - delete record,
    - build variables list for a stream, check how many files are on disk,
      on db and how many are expected for each of them
    - check missing files for a variable
    """
    
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)
    ctx.obj['log'] = cdslog
    ctx.obj['dsargs'] = define_args(index, tstep)
    if not prod:
        prod = ctx.obj['dsargs']['product_type']
    prod = expand_prod(prod)
    if action == 'update':
        for pr in prod:
            update_db(cfg, index, tstep, pr, list(experiment), list(model))
    elif action == 'delete':    
        if len(prod) > 1:
            cdslog.info(f"{len(prod)} products were passed as argument, pass only one")
            sys.exit()
        delete_record(cfg, index, prod[0], tstep, list(experiment), list(model))
    elif action == 'list':    
        #varlist = [] 
        models_stats(ctx, cfg, index, tstep, prod, list(param), list(model), verbose)
    elif action == 'intake':    
        #varlist = [] 
        create_intake(cfg)

if __name__ == '__main__':
    cds(obj={})
