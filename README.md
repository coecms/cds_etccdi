[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.6897993.svg)](https://doi.org/10.5281/zenodo.6897993)

# cds_etccdi

The cds_etccdi python code is an interface to the CDS api to download the [Climate extreme indices and heat stress indicators derived from CMIP6 global climate projections](https://cds.climate.copernicus.eu/cdsapp#!/dataset/sis-extreme-indices-cmip6?tab=overview)
( CICERO_ETCCDI ) dataset from the Copernicus Climate Data Store (CDS).
It uses a modified version of the [CDS api](https://cds.climate.copernicus.eu/api-how-to) which stops after a request has been submitted and executed. The target download url is saved and downloads are run in parallel by the code using the Pool multiprocessing module. As well as managing the downloads the code gets all the necessary information on available variables from local json configuration files. <br>
Before submitting a request the code will check that the file is not already available locally by quering a sqlite database. After downloading new files it is important to update the database to avoid downloading twice the same file. Files are first downloaded in a staging area, as the files come as archives, then `untarred` and moved to the final destination.

## Getting started

### Downloading

```code
    cds download -i etccdi -t yr -pt b1961_1990 [-e historical -m access_cm2 -p cold_days -q] 
```
 where the following arguments are required:
   - `i/index` is the type of index etccdi or hsi
   - `t/tstep` is the timstep (yr/mon/day)
   - `pt/product` is the kind of product in the example with base period 1961-1990
And these arguments are optional.
   - `e/experiment` - the experiment of the input CMIP6 data
   - `m/model` - the model of the input CMIP6 data
   - `p/param` - a specific variable name
   - `q/queue` - is a flag to defer the download and create a request file instead


#### Downloading using existing request file

If you defer the download using the `queue` flag you can submit the request later using the `scan` subcommand.

```code
   cds scan -f cds_request_20220704031336.json
```
The request file is a json file that contains the arguments passed to the code previously.
We used this option when doing a bulk downloads, so all requests can then be run and managed by a cron job.<br>
This repository contains an example of the bash wrapper we used: cds_wrapper.sh.

### Updating the database

  
```code
   cds db [-i etccdi -t yr -pt b1961_1990]
```

It will update the database looking specifically for files fitting the constraints.<br>
All arguments are optional, however running without any will scan the all directories and could be slower.
The `db` sub-command is by default updating (or creating if not existing yet) a database of all the files already downloaded.<br>
Other functionalities are also available including the creation of a file list that can be used to setup an intake catalogue.
    
### Other options

To see all the available options and arguments

```code
   cds --help
   cds <sub-command> --help
```

where sub-command are `download`, `scan` and `db`
   
