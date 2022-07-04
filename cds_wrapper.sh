#!/bin/bash
#
# Wrapper script (called by cron)
#
# Set the directories 
# Usage: cds_wrapper.sh <priority>

# set some vars
LOCKFILE="/tmp/cds_wrapper.lock"
TSTAMP=$(date -u +%Y%m%dT%H%M%S)
SCRIPTDIR=$(dirname $0)
cd $SCRIPTDIR
ERRORLOG="/mnt/pvol/etccdi/log/cds_wrapper_error.log"
REQUESTDIR="/mnt/pvol/etccdi/Requests/${1}"
COMPLETEDIR="/mnt/pvol/etccdi/Requests/Completed"

echo "--- Starting $0 ($TSTAMP) ---"

# set exclusive lock
echo "Setting lock ..."
LOCKED="NO"
exec 8>$LOCKFILE
#flock -nx 8 || exit 1
flock -nx 8 || LOCKED="YES"
if [ "$LOCKED" == "YES" ] ; then
  echo "  already locked - exiting!"
  exit 1
fi


# refresh requests
# couple of options: git pull; or rsync ; or nothing
echo "Checking for new requests, priority: $1"
REQUESTS="$(ls $REQUESTDIR/cds_request*.json)"

source /home/ubuntu/miniconda3/etc/profile.d/conda.sh
conda activate era5env 

# loop through list of request files and run the download command
echo "Starting download ..."
for J in $REQUESTS ; do
  echo "  $J"
  #~/.local/bin/era5 scan -f $J 1>/dev/null 2>>$ERRORLOG
  cds  scan -f $J 1>/dev/null 2>>$ERRORLOG
  echo " Finished , moving request $J"
  mv $J ${COMPLETEDIR}/$(basename "$J")
  #python3 cli.py scan -f $J
done

# update sqlite db
echo "Updating database ..."
cds db -i etccdi -t mon
cds db -i etccdi -t yr
cds db -i hsi -t day

echo "--- Done ---"
