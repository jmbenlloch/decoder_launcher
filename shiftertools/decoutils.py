import json

from os   import path
from time import time
from time import sleep

from shiftertools.produtils import wait_for_finished_jobs
from shiftertools.produtils import check_file_exists
from shiftertools.produtils import execute_job

from shiftertools.utils import Messenger
from shiftertools.paths import rawdata_files
from shiftertools.paths import make_run_dirs
from shiftertools.paths import decoder_files_path
from shiftertools.paths import decoder_jobs_path
from shiftertools.paths import decoder_logs_path


def create_decoder_config(config_file, rawfile, hdf5file, hdf5file2=''):
    """ Create decoder job file: from DATE to hdf5 format """

    params = {"file_in"   : rawfile,
              "file_out"  : hdf5file,
              "two_files" : False,
              "no_db"     : True,
              "host"      : "localhost",
              "user"      : "next",
              "pass"      : "Canfranc",
              "dbname"    : "NEWDB",
              'split_trg' : False }
    
    if hdf5file2: 
        params['split_trg'] = True
        params['file_out2'] = hdf5file2
    
    with open(config_file, 'w') as fileout:
        json.dump(params, fileout, indent=4)


def create_decoder_job(job_file, config_file, job_name, queue, stdout_file, stderr_file):
    """ Create decoder batch job file """

    template = '''
#PBS -N {job_name}
#PBS -q {queue}
#PBS -Roe
#PBS -l mem=1gb
singularity run --bind /volume0/rawdata:/volume0/rawdata --bind /volume0/analysis:/volume0/analysis /volume0/software/rawdata.sif {config_file} 1> {stdout_file} 2> {stderr_file}

FILEOUT=`sed 's/,/\\n/g' {config_file} | grep '"file_out"' | cut -d':' -f 2 | cut -d'"' -f2`"
FILEOUT2=`sed 's/,/\\n/g' {config_file} | grep '"file_out2"' | cut -d':' -f 2 | cut -d'"' -f2`"

function remove_empty_files {{
    CHECKSIZE=`h5ls -r $1/Run/events | grep "NOT FOUND" | wc -l`
    if [ $CHECKSIZE -gt 0 ]; then
        rm $1;
    fi
}}

remove_empty_files $FILEOUT
remove_empty_files $FILEOUT2

# To avoid error in PBS
exit 0
'''
    
    with open(job_file, 'w') as fileout:
        fileout.write(template.format(**locals()))
         


def decode_file(run, raw_file, queue, cmd='qsub', reproc=False, split=False):
    fnumber = int(raw_file.split(".")[-2])
    ldc     =     raw_file.split(".")[-4][:4]

    if split:
        basename = 'run_{}_{:04d}_{}_trigger{}_waveforms.h5'
        hdf5_file  = path.join(decoder_files_path(run), basename.format(run, fnumber, ldc, 1))
        hdf5_file2 = path.join(decoder_files_path(run), basename.format(run, fnumber, ldc, 2))
    else:
        basename = 'run_{}_{:04d}_{}_waveforms.h5'
        hdf5_file  = path.join(decoder_files_path(run), basename.format(run, fnumber, ldc))
        hdf5_file2 = ''

    config_basename = 'run_{}_{:04d}_{}_waveforms.json'
    job_basename    = 'run_{}_{:04d}_{}_waveforms.sh'
    stdout_basename = 'run_{}_{:04d}_{}_waveforms.out'
    stderr_basename = 'run_{}_{:04d}_{}_waveforms.err'

    config_file = path.join(decoder_jobs_path(run), config_basename.format(run, fnumber, ldc))
    job_file    = path.join(decoder_jobs_path(run), job_basename   .format(run, fnumber, ldc))
    stdout_file = path.join(decoder_logs_path(run), stdout_basename.format(run, fnumber, ldc))
    stderr_file = path.join(decoder_logs_path(run), stderr_basename.format(run, fnumber, ldc))
    job_name    = 'decoder_{}_{}_{}'.format(run, fnumber, ldc)
    
    m = Messenger('decoutils')
    m.log(f"Raw data to be processed: file {fnumber} from run {run}")
    m.log(f"Raw data location: {raw_file}")
    m.log(f"HDF5 output data file: {hdf5_file}")
    if hdf5_file2: m.log(f"HDF5 output data file: {hdf5_file2}")
    m.log(f"Decoder params to be used: {config_file}")
    m.log(f"Batch job file to be used: {job_file}")
    m.log(f"Output log file: {stdout_file}")
    m.log(f"Error log file: {stderr_file}")

    check_file_exists(hdf5_file, reproc, m.log)
    make_run_dirs(run)
    create_decoder_config(config_file, raw_file, hdf5_file, hdf5_file2)
    create_decoder_job(job_file, config_file, job_name, queue, stdout_file, stderr_file)

    return execute_job(cmd, job_file, m.log)
        

def decode_file_as_available(run, queue, max_qjob, stime = 60, split = False): 
    files_added = []
    m = Messenger('decoutils')
    m.log("Waiting for new raw bin data...")
    while True:
        files = rawdata_files(run)
        if files:
            dtime = time() - path.getmtime(files[-1])
            if dtime < stime: files.pop()
        new_files = [f for f in files if f not in files_added]
        if new_files: 
            m.log("New {} raw data files found".format(len(new_files)))
            for fname in new_files: 
#                wait_for_finished_jobs(max_qjob, f'decoder_{run}', stime, m.log)
                decode_file(run, fname, queue,"qsub", reproc = False, split = split)
            m.log("Waiting for new raw data...")
        files_added = files
        sleep(stime) 
    return
