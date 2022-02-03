import os
import re

from glob import glob

from shiftertools.paths import rawdata_files
from shiftertools.paths import decoder_files_path
from shiftertools.paths import decoder_logs_path
from shiftertools.utils import Messenger
from shiftertools.utils import error


def check_decoder_production(run):
    m = Messenger('prodchecks')
    m.log(f"Checking decoder production for run {run}")

    raw_files = rawdata_files(run)
    h5_files  = glob(decoder_files_path(run) + '/*h5')
    err_files = glob(decoder_logs_path (run) + '/*err')

    if not raw_files : error("No raw files found")
    if not h5_files  : error("No prod files found")
    
    import pdb
    pdb.set_trace()
    extract_raw_fnumber = lambda f: int(f.split('_')[2].split('.')[-1]) 
    hfiles = sorted(hfiles, key = extract_raw_fnumber)
    nfiles = [extract_raw_fnumber(f) for f in hfiles] 
    
    extract_raw_fnumber =  lambda f: int(f.split('.')[-2])
    rfiles = sorted(rfiles, key = ksort)
    nrfiles = [ksort(f) for f in rfiles]
        
    notproc = []
    if len(nrfiles)!=len(nfiles):
        notproc = set(nrfiles)-set(nfiles)
        notproc=sorted(notproc)
        
    errors = [log.split('_')[2] for log in lfiles if os.stat(log).st_size]

    m.log("Total number of gdc1 bin files in run %i: %i"%(RUN,len(rfiles)))
    m.log("Total number of decoded files: %i"%len(hfiles))
    m.log("Total number of not decoded files: %i"%len(notproc))
    m.log("Total number of missing log files: %i"%(len(hfiles)-len(lfiles)))
    m.log("Total number of jobs with errors: %i"%len(errors))

    if notproc:
        fileout = f'FilesNotProcessed_Run{run}.txt'
        with open(fileout, 'w') as fd:
            for fnumber in notproc:
                fd.write(f"{fnumber}\n")
        m.log(f"Not processed files listed in: {fileout}")
    
    if errors:
        fileout = f"JobsWithErrors_Run{run}.txt"
        with open(fileout, 'w') as fd:
            for log_index in errors:
                fd.write(f"{log_index}\n")
        m.log(f"Logs with errors listed in: {fileout}")
        
    # check size of files
    extract_h5_fnumber = lambda f: f.split('_')[2]
    empty_h5_files = [fname for fname in h5_files if os.stat(fname).st_size < 600000]
    if empty_h5_files: 
        m.log("Number of empty decoded files: {}".format(len(empty_h5_files)))
        fileout = f'RWFEmptyFiles_Run{run}.txt'
        with open(fileout,'w') as fd:
            for h5file in empty_h5_files:
                fd.write('{}\n'.format(extract_h5_fnumber(h5file)))
        m.log(f"empty decoded files listed in: {fileout}")
