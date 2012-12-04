desdb
=====

DES file locations and database access


Installation
------------

### dependencies 

Note you must first install the oracle libraries and the cx_Oracle python
library.  Because the official version of cx_Oracle for OSX is broken, we have
bundled a patched version in the following files. Choose the one for your
architechture.  See the instructions in the README for installation

    http://www.cosmo.bnl.gov/www/esheldon/code/misc/des-oracle-linux-x86-64-v1.tar.gz
    http://www.cosmo.bnl.gov/www/esheldon/code/misc/des-oracle-macosx-x86-64-v1.tar.gz
    http://www.cosmo.bnl.gov/www/esheldon/code/misc/des-oracle-macosx-i386-v1.tar.gz


### code install

Get the source .tar.gz file, untar the file, cd into
the created directory.  To install in the "usual" place

    python setup.py install

To install under a particular prefix

    python setup.py install --prefix=/some/directory
