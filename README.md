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

There are some scripts with pre-defined queries.  After installation these
will be in your path.

* get-table-info: Print the column names,typecode,typename,precision,scale,value
    for the input table.
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

Downloading Data
----------------

There are scripts to download files using wget.  After installation these
will be in your path.


* wget-des: download files from the des web site.
* wget-des-parallel: download files in parallel
    Requires the "parallel" program. http://www.gnu.org/software/parallel/

    The parallelization occurs across DES runs, for example if you want
    to download multiple "red" runs you can use this script.

Note you need the DESDATA environment variable set to the location of your DES
data locally.  You need the DESREMOTE set to the remote directory (see the DES
wiki to get the current URL
https://cdcvs.fnal.gov/redmine/projects/des-sci-verification/wiki/Access)


Files
-----

The sub-module "files" has code get standard file names and locations on your
system, the remote site, and in the database.  The class DESFiles is defined to
make this easy.  Examples

    # print the local path to a red image
    import desdb

    df=desdb.DESFiles()
    type='red_image'
    run='20110829231419_20110802'
    expname='decam--18--38-i-2'
    ccd=3

    print df.url(type=type, run=run, expname=expname, ccd=ccd)
    /global/project/projectdirs/des/wl/DES/red/20110829231419_20110802/red/decam--18--38-i-2/decam--18--38-i-2_03.fits.fz

    # get the remote location of the red image by using the 'net' file system.
    df=desdb.DESFiles(fs='net')
    print df.url(type=type, run=run, expname=expname, ccd=ccd)
    ftp://the.des.server/DESFiles/desardata/DES/red/20110829231419_20110802/red/decam--18--38-i-2/decam--18--38-i-2_03.fits.fz

where "the.des.server" will be replaced by the actual current server. 

Note you need the DESDATA environment variable set to get the full path to your
local file.  You need the DESREMOTE variable set to get the remote directory
(see the DES wiki to get the current URL
https://cdcvs.fnal.gov/redmine/projects/des-sci-verification/wiki/Access)

Other classes of interest are the Red and Coadd classes for dealing with those
file types.

Connection Class
------------------
In the sub-module "desdb" we define the Connection class, which inherits from
the cx_Oracle connection.  See the scripts above for examples of how
to use this class.

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

### code install

Get the source .tar.gz file, untar the file, cd into
the created directory.  To install in the "usual" place

    python setup.py install

To install under a particular prefix

    python setup.py install --prefix=/some/directory

### dependencies 

Note you must first install the oracle libraries and the cx_Oracle python
library.  For linux you can download and install the default versions if you
want, or use the bundles listed below.  However, the official version of
cx_Oracle for OSX is broken on recent versions of the operating syste. Thus we
have bundled a patched version in the macosx files below. Choose the one for
your architecture.

    http://www.cosmo.bnl.gov/www/esheldon/code/misc/des-oracle-linux-x86-64-v2.tar.gz
    http://www.cosmo.bnl.gov/www/esheldon/code/misc/des-oracle-macosx-x86-64-v2.tar.gz
    http://www.cosmo.bnl.gov/www/esheldon/code/misc/des-oracle-macosx-i386-v2.tar.gz

Download the file and untar it using

    tar xvfz des-oracle-linux-x86-64-v2.tar.gz

cd into the directory and run

    ./do-install $dir

Where $dir is the location you want the install; this can be anywhere.
Then source the setup file appropriate for your shell.

    source $dir/setup.sh   # for bash
    source $dir/setup.csh  # for csh/tcsh

You can put that into your startup file, e.g. ~/.bashrc or ~/.cshrc etc.

