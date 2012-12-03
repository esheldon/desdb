import glob
from distutils.core import setup

scripts=glob.glob('desdb/bin/*.py')
scripts += ['desdb/bin/des-query']

setup(name="desdb", 
      version="0.1.0",
      description="DES file locations and database access",
      license = "GPL",
      author="Erin Scott Sheldon",
      author_email="erin.sheldon@gmail.com",
      packages=['desdb'], scripts=scripts)




