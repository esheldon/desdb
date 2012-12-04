import os
import glob
from distutils.core import setup

scripts= ['des-query',
          'get-coadd-info',
          'get-coadd-srclists',
          'get-filelist',
          'get-red-info',
          'get-release-runs',
          'get-table-info',
          'wget-des',
          'wget-des-parallel',
          'des-query']

scripts=[os.path.join('desdb','bin',s) for s in scripts]

setup(name="desdb", 
      version="0.1.0",
      description="DES file locations and database access",
      license = "GPL",
      author="Erin Scott Sheldon",
      author_email="erin.sheldon@gmail.com",
      packages=['desdb'], scripts=scripts)




