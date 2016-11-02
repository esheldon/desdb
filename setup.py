import os
import glob
from distutils.core import setup

scripts= ['des-query',
          'des-sync-red',
          'des-sync-coadd',
          'des-rsync-red',
          'des-rsync-coadd',
          'des-fits2table',
          'des-red-expnames',
          'get-coadd-info-by-release',
          'get-coadd-info-by-run',
          'get-coadd-srclist',
          'get-coadd-srcruns-by-run',
          'get-coadd-srcruns-by-release',
          'get-release-filelist',
          'get-red-info-by-release',
          'get-release-runs']

scripts=[os.path.join('desdb','bin',s) for s in scripts]

setup(name="desdb", 
      version="0.1.0",
      description="DES file and database access",
      license = "GPL",
      author="Erin Scott Sheldon",
      author_email="erin.sheldon@gmail.com",
      packages=['desdb'],
      scripts=scripts)
