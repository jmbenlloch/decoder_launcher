import os

from glob import glob
from os   import path

def rawdata_files(run):
    path  = f'/rawdata/{run}/run_{run}.ldc*next.next-100.*.rd'
    files = glob(path)
    files.sort(key = lambda f: int(f.split('.')[-2]))
    return files

base_path = '/analysis/{run}/hdf5/'

def decoder_files_path(run):
    return path.join(base_path.format(run=run), 'data')

def decoder_jobs_path(run):
    return path.join(base_path.format(run=run), 'jobs')

def decoder_logs_path(run):
    return path.join(base_path.format(run=run), 'logs')

def make_run_dirs(run):
    try:
        os.makedirs(decoder_files_path(run))
        os.makedirs(decoder_jobs_path(run))
        os.makedirs(decoder_logs_path(run))
    except FileExistsError:
        pass
