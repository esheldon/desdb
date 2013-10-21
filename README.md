desdb
=====

DES files and database access

Note this package is in flux at the moment as things change in DESDM.  Also, we
have moved to using ~/.netrc for authentication for both database access and
file downloads.

News
----

A new patched version of cx_oracle and the oracle libraries for Mac OS X is
available in the dependencies section (version 3).  This version should work
on 10.8

Connection Class
------------------

In the sub-module "desdb" we define the Connection class, which inherits from
the cx_Oracle connection.  We also provide scripts that that use this class
for database queries.

Generic Query Script
--------------------

After installation, the script des-query will be in your path.  You can send
queries on standard input or via the -q option

    des-query -q query
    des-query < file
    cat file | des-query

By default the format is csv.  You can control this with the -f/--format
option.  Possibilities are csv,space,json,pretty,pyobj.  pretty is a formatted
in nicely for viewing but is not good for machine reading.  pyobj can be read
from python using eval

examples

    # Get some object data
    des-query -q "select xwin_image_r,ywin_image_r from coadd_objects where rownum < 10"

    # Get the runs associated with release "dr012" and files of type "red"
    des-query -q "select distinct(run) from dr012_files where filetype='red'"

    des-query < sql_file > output.csv

    des-query -f json < sql_file > output.json

    # list all the tables
    des-query -l

    # describe a table
    des-query -d coadd_objects

    # describe with column comments
    des-query -c -d coadd_objects

Pre-fab queries
---------------

There are some scripts with pre-defined queries.  After installation these
will be in your path (there may be more than listed here)

run based queries
* des-red-expnames: Print the exposurenames for the given "red" run
* get-coadd-srcruns-by-run: list the input source "red" runs that were
  used to make the indicated coadd

release based queries (e.g. dr012)
* get-coadd-info-by-release: get allthe coadd info for the release
* get-coadd-srclists-by-release: list all red images and catalogs that
  went into all coadds for the indicated release
* get-red-info-by-release: Look up all red catalogs and images in the 
  input release and write out their file ids, path info, and external url.
* get-release-filelist: Look up the listed file types and write out their 
  local path information.
* get-release-runs: list all runs for the indicated file types in the
  indicated release

Downloading Data
----------------

There are scripts to sync files using wget.  After installation these will be
in your path.


* des-sync-red: download red images and catalogs
* des-sync-coadd: download coadd images and catalogs

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
    https://des.file.server/DESFiles/desardata/OPS/red/20110829231419_20110802/red/decam--18--38-i-2/decam--18--38-i-2_03.fits.fz

where "des.file.server" will be the actual server you are using. 

Note you need the DESDATA environment variable set to get the full path to your
local file.  You need the DESREMOTE variable set to get the remote directory

Other classes of interest are the Red and Coadd classes for dealing with those
file types.

Access to Servers
-----------------

You can send your username and password via -u/--user and -p/--password, but it
is easier to use a netrc file.  

There are two machines, the file server and the database server

    machine des.file.server login your_username password your_password
    machine des.database.server login your_username password your_password

The actual server names should be replace by the actual the current ones.  and
"your_username" and "your_password" should be replaced with your login info.
And make sure the file is not readable or writable by others.

    chmod go-rw ~/.netrc

This is enforced.

To get the current file server, see the DES wiki
https://cdcvs.fnal.gov/redmine/projects/des-sci-verification/wiki/Access

To get the current database server, see

Installation
------------

### code install

Get the source .tar.gz file, untar the file, cd into
the created directory.  To install in the "usual" place

    python setup.py install

To install under a particular prefix

    python setup.py install --prefix=/some/directory

### dependencies 

For file downloads you only need this package and wget.


For database queries, you need to install the oracle libraries and the
cx_Oracle python library.  For linux you can download and install the default
versions.

The version of cx_Oracle for OSX is broken on recent versions of the operating
system. Thus we have bundled a patched version in the macosx files below.
Choose the one for your architecture.  Every update to the operating system
over the last couple of years has broken this module, so any feedback you can
give is very welcome, especially if you can work out a general strategy for
compile flags on all versions of the OS

    http://www.cosmo.bnl.gov/www/esheldon/code/misc/des-oracle-linux-x86-64-v2.tar.gz
    http://www.cosmo.bnl.gov/www/esheldon/code/misc/des-oracle-macosx-x86-64-v3.tar.gz
    http://www.cosmo.bnl.gov/www/esheldon/code/misc/des-oracle-macosx-i386-v2.tar.gz

Download the file and untar it using

    tar xvfz des-oracle-linux-x86-64-v2.tar.gz

cd into the directory and run

    ./do-install $dir

Where $dir is the full path to the location you want the install; this can be
anywhere.  Then source the setup file appropriate for your shell.

    source $dir/setup.sh   # for bash
    source $dir/setup.csh  # for csh/tcsh

You can put that into your startup file, e.g. ~/.bashrc or ~/.cshrc etc.

