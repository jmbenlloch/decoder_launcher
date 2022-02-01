import os
from glob import glob
import re

import shiftertools
from shiftertools.utils import *
from shiftertools.produtils import *


def checkDecoProd(RUN):
    
    m = Messenger('prodchecks')
    m.log("Checking decoder production for run",RUN)

    rpattern = shiftertools.RAWDATAWILDCARDPATH%(RUN,RUN) # gdc1 raw files
    #hpattern = shiftertools.HDF5DATAWILDCARDPATH%(RUN,RUN)       
    hpattern = shiftertools.HDF5TRIGDATAWILDCARDPATH%(RUN,RUN,1) # trigger1 hdf5 files
    lpattern = '/run_%04i_*_waveforms.h5.err'%RUN               
    lpattern = shiftertools.LOGPATH%(RUN)+lpattern               # log files
    rfiles = glob(rpattern)
    hfiles = glob(hpattern)
    lfiles = glob(lpattern)

    if not rfiles: error("No raw files found with pattern %s"%rpattern)
    if not hfiles: error("No prod files found with pattern %s"%hpattern)
    
    ksort = lambda f: int(f.split('_')[2].split('.')[-1]) 
    hfiles = sorted(hfiles, key = ksort)
    nfiles = [ksort(f) for f in hfiles] 
    
    ksort =  lambda f: int(f.split('.')[-2])
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
        ofile = 'FilesNotProcessed_Run%i.txt'%RUN
        f = open(ofile,'w')
        for ifile in notproc: f.write(str(ifile)+'\n')
        f.close()
        m.log("Not processed files listed in: %s"%ofile)
    
    if errors:
        ofile = "JobsWithErrors_Run%i.txt"%RUN
        f = open(ofile,'w')
        for ilog in errors: f.write(ilog+'\n')
        f.close()
        m.log("Logs with errors listed in: %s"%ofile)
        
    # check size of files
    ehfiles = [h for h in hfiles if os.stat(h).st_size<600000]
    if ehfiles: 
        m.log("Number of empty decoded files:",len(ehfiles))
        ofile = 'RWFEmptyFiles_Run%i.txt'%RUN
        with open(ofile,'w') as f:
            for ifile in ehfiles: f.write(ifile.split('_')[2]+'\n')
        m.log("empty decoded files listed in: %s"%ofile)
