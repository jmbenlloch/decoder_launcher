import os,os.path
from subprocess import call,check_output
from time import sleep,ctime,time
from glob import glob
import shiftertools
from shiftertools.utils import *
import magic

from shiftertools.paths import rawdata_files


def getRDFile(RUN,FILE): 
    return shiftertools.RAWDATAFILEPATH%(RUN,RUN,FILE)

def getRDWildCard(RUN): 
    return shiftertools.RAWDATAWILDCARDPATH%(RUN,RUN)

import pdb

def getRDFiles(run_number):
    pdb.set_trace()
    rawdatawc = getRDWildCard(run_number)
    return sorted(glob(rawdatawc))

def getRWFWildCardFile(RUN):
    return shiftertools.HDF5DATAWILDCARDPATH%(RUN,RUN)

def getRWFTrigWildCardFile(RUN,TRIG):
    return shiftertools.HDF5TRIGDATAWILDCARDPATH%(RUN,RUN,TRIG)

def getRWFMonWildCardFile(RUN):
    return shiftertools.WFMONWILDCARDPATH%(RUN,RUN)

def getRWFFiles(RUN,TRIG=0):
    #if TRIG: rwfc = getRWFTrigWildCardFile(RUN,TRIG)
    #else: rwfc = getRWFWildCardFile(RUN)
    rwfc = getRWFTrigWildCardFile(RUN,TRIG) if TRIG else getRWFWildCardFile(RUN)
    
    return sorted(glob(rwfc))

#------------------------#

def create_folders(run):
    
    """ Create directories for HDF5 files """

    dirs = []
    dirs.append(shiftertools.RECODATARUNPATH%run)
    dirs.append(shiftertools.HDF5PATH%run)
    dirs.append(shiftertools.JOBPATH%run)
    dirs.append(shiftertools.LOGPATH%run)
    dirs.append(shiftertools.HISTOPATH%run)
    dirs.append(shiftertools.MAPPATH%run)
    for d in dirs: call('mkdir -p %s'%(d),shell=True)

    return 


def execute_job(cmd, job, log = stprint):
    """ Execute job """

    cmd = f'{cmd} {job}'
    log("Running job:")
    log(f"   {cmd}")
    fail = call(cmd, shell=True)
    if fail: error("Executing failed!")

#------------------------#

def closedFile(f,stime=180,msize=10000):
    try:
        timeok = time()-os.path.getmtime(f)>stime
        sizeok =  os.stat(f).st_size>msize
        fileok = 'Hierarchical' in magic.Magic().from_file(f)
        return timeok and sizeok and fileok
    except: # file has been removed because empty
        return False
    
def check_file_exists(ofile,reproc, log = stprint):
    if os.path.isfile(ofile): 
        log("Output file already exists: %s"%ofile)
        if reproc: log("   ...reprocessing file")
        else: error("Reprocessing not allowed")

def checkExistingFileSkip(ofile, reproc, log = stprint):
    if os.path.isfile(ofile): 
        log("Output file already exists: %s"%ofile)
        if reproc:
            log("   ...reprocessing file")
            return True
        else:
            stprint("skipping file process step")
            return False
    return True

def get_jobs_in_queue(label): 
    cmd = f'qstat -a | grep {label} | wc -l'
    out = check_output(cmd, shell=True, executable='/bin/bash')
    return int(os.fsdecode(out))

def wait_for_finished_jobs(max_jobs, label, stime, log = stprint):
    njob = get_jobs_in_queue(label)
    if njob >= max_jobs: 
        log(f"Number of jobs in queue at limit ({max_jobs})")
        log("Waiting for some job to finish...")
        while njob >= max_jobs:
            njob = get_jobs_in_queue(label)
            if njob >= max_jobs: sleep(stime)

def waitForNoJobs(LABEL,STIME=60): 
    while getJobsInQueue(LABEL): sleep(STIME)  

def waitForProdEnd(LABEL,log = stprint):
    log("ALL JOBS SUBMITTED!")
    log("Waiting for all jobs to finish...")
    try: 
        waitForNoJobs(LABEL)
        log("")
        log("ALL JOBS FINISHED!")
        log("")
    except KeyboardInterrupt: 
        log("OK, so I don't wait any longer... Bye!")
