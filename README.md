desdb
=====

DES file locations and database access

Generic Queries
---------------

After installation, the script des-query will be in your path.  You
can send queries on standard input or via the -q option

    des-query -q query
    des-query < file
    cat file | des-query

By default the format is csv.  You can control this withthe -f/--format option.
Possibilities are csv,json,pretty,pyobj.  pretty is a formatted in nicely for
viewing but is not good for machine reading.  pyobj can be read from python
using eval

examples

    # Get the runs associated with release "dr012" and files of type "red"
    des-query -q "select distinct(run) from dr012_files where filetype='red'"

    des-query < sql_file > output.csv

    des-query -f json < sql_file > output.json

Pre-fab queries
---------------

There are some scripts with pre-defined queries.

* get-table-info: For the input table name, print the 
    column names,typecode,typename,precision,scale,value.
* get-release-runs: Print all runs for input release and file type.
* get-red-info: Look up all red catalogs and images in the input release
    and write out their file ids, path info, and external url.
* get-coadd-info: Look up all coadd images in the input release and write out their file ids,
    along with some other info.
* get-coadd-srclists: Look up all coadd images for the requested release, find the
    single epoch 'red' images that were used as input, and write out a json file
    with the coadd and red image info.  The json file is keyed by coadd_id.
* get-filelist: Look up the listed file types and write out their local path information.  The
    types should be a comma separated list.


Preparation
-----------

You can send your username and password via -u/--user and -p/--password, but it
is easier to use the password file.  Put your des database username and
password in a file ~/.desdb_pass

    username
    pass

And make sure the file is not readable or writable to others

    chmod go-rw ~/.desdb_pass

Installation
------------

### dependencies 

Note you must first install the oracle libraries and the cx_Oracle python
library.  Because the official version of cx_Oracle for OSX is broken, we have
bundled a patched version in the following files. Choose the one for your
architecture.

    http://www.cosmo.bnl.gov/www/esheldon/code/misc/des-oracle-linux-x86-64-v1.tar.gz
    http://www.cosmo.bnl.gov/www/esheldon/code/misc/des-oracle-macosx-x86-64-v1.tar.gz
    http://www.cosmo.bnl.gov/www/esheldon/code/misc/des-oracle-macosx-i386-v1.tar.gz

Install with

    ./do-install directory

And follow the instructions for setting your paths.

### code install

Get the source .tar.gz file, untar the file, cd into
the created directory.  To install in the "usual" place

    python setup.py install

To install under a particular prefix

    python setup.py install --prefix=/some/directory
