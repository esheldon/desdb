"""
Based partly on code in the DES trivialAccess.py
"""

import os
import sys
from sys import stdout,stderr
import csv

import cx_Oracle

try:
    import json
    have_json=True
except:
    have_json=False
try:
    import cjson
    have_cjson=True
except:
    have_cjson=False

_url_template = "%s:%s/%s"

_defhost = 'leovip148.ncsa.uiuc.edu'
_defport = 1521
_defdb = 'dessci'

_release_map={'dc6b':'dr012', 'dr012':'dr012'}

_PREFETCH=10000

# for numpy conversions of NUMBER types
_defs={}
_defs['f4_digits'] = 6
_defs['f8_digits'] = 15
_defs['lower'] = True

_binary_err='size of %s not allowed for BINARY floating point types'

_flt_digits_err=\
"""WARNING: Digits for field "%s" of %s exceeds that of an
8-byte floating point
Setting to type "f16" which may or may not exceed 8-bytes in size, 
depending on your platform\n"""

_int_digits_err=\
"""WARNING: Digits for field "%s" of %s exceeds largest available 
(18 digits for 8-byte binary integer).  Setting to 8-byte integer\n"""

_string_err='The size of field "%s" is %s but must be greater than zero'


def dataset2release(dataset):
    if dataset not in _release_map:
        raise ValueError("Unknown data set '%s'" % dataset)
    return _release_map[dataset]

def connect(**keys):
    return Connection(**keys)

class Connection(cx_Oracle.Connection):
    """
    A simple wrapper to the cx_oracle connection object.

    Simplifies access to DES db.

    methods in addition to those for the cx_oracle connection object
    ----------------------------------------------------------------

    quick:
        Execute the query and return the results.

    quickWrite:
        Execute the query and write the results to a the
        standard output or a file.

    describe:
        Print a description of the specified table.

    list_tables:
        List all available tables, as available in the all_tables table.
    """
    def __init__(self, **keys):
        """
        parameters
        ----------
        user: optional
            Username. By default gotten from netrc
        password: optional
            Password. By default gotten from netrc
        host: optional
            over-ride the default host
        port: optional
            over-ride the default port
        dbname: optional
            over-ride the default database name
        """
        p=PasswordGetter(**keys)
        self._pwd_getter=p

        self._process_pars(**keys)

        url = _url_template % (p.host, self._port, self._dbname)

        cx_Oracle.Connection.__init__(self,p.user,p.password,url)


    def quick(self, query, lists=False, strings=False, array=False,
              show=False, **keys):
        """
        Execute the query and return the result.

        By default returns a list of dicts.

        parameters
        ----------
        query: string
            A query to execute
        lists: bool, optional
            Return a list of lists instead of a list of dicts.
        strings: bool, optional
            Convert all values to strings
        array: bool, optional
            If True, convert to a numpy recarray
        show: bool, optional
            If True, print the query to stderr
        """

        curs=self.cursor()

        # pre-fetch
        curs.arraysize = _PREFETCH

        if show: 
            stderr.write(query);stderr.write('\n')
        curs.execute(query)

        if curs.description is not None:

            if lists:
                res=[]
                try:
                    for r in curs:
                        res.append(r)
                except KeyboardInterrupt:
                    curs.close()
                    raise RuntimeError("Interrupt encountered")

            elif array:
                res=cursor2array(curs)
            else:
                res = cursor2dictlist(curs)
        else:
            res=None
        curs.close()
        return res

    def quickWrite(self, query, fmt='csv', header='names',
                   file=sys.stdout, show=False):
        """
        Execute the query and print the results.

        parameters
        ----------
        query: string
            A query to execute
        fmt: string, optional
            The format for writing.  Default 'csv'
        header: string,optional
            If not False, put a header.  Can be
                'names' csv names
                others?
        file: file object, optional
            Write the results to the file. Default is stdout
        show: bool, optional
            If True, print the query to stderr
        """

        curs=self.cursor()
        curs.arraysize = _PREFETCH

        if show: 
            stderr.write(query)
        curs.execute(query)

        if curs.description is not None:
            print_cursor(curs, fmt=fmt, header=header, file=file)
        curs.close()

    def describe(self, table, fmt='pretty', comments=False, show=False):
        """
        Print a simple description of the input table.
        """
        # separate queries because the fgetmetadata is a slow
        # function call
        if not comments:
            #q="""
            #    SELECT
            #        column_name, 
            #        CAST(data_type as VARCHAR2(15)) as type, 
            #        CAST(data_length as VARCHAR(6)) as length, 
            #        CAST(data_precision as VARCHAR(9)) as precision, 
            #        CAST(data_scale as VARCHAR(5)) as scale
            #    FROM
            #        all_tab_columns
            #    WHERE
            #        table_name = '{table}'
            #        AND column_name <> 'TNAME'
            #        AND column_name <> 'CREATOR'
            #        AND column_name <> 'TABLETYPE'
            #        AND column_name <> 'REMARKS'
            #    ORDER BY 
            #        column_id
            #"""
            q="""
                SELECT 
                    column_name,
                    CAST(data_type as VARCHAR2(15)) as type, 
                    CAST(data_length as VARCHAR(6)) as length, 
                    CAST(data_precision as VARCHAR(9)) as precision, 
                    CAST(data_scale as VARCHAR(5)) as scale
                FROM
                    table(fgetmetadata)
                WHERE
                    table_name  = '{table}'
                ORDER BY
                    column_id
            """

        else:
            q="""
                SELECT 
                    column_name,
                    data_type as type,
                    data_length as length,
                    data_precision as precision,
                    data_scale as scale,
                    comments
                FROM
                    table(fgetmetadata)
                WHERE
                    table_name  = '{table}'
                ORDER BY
                    column_id
            """
        q=q.format(table=table.upper())

        if show:
            stderr.write(q)

        curs = self.cursor()
        curs.arraysize = _PREFETCH

        curs.execute(q) 
        print_cursor(curs,fmt=fmt)

        # now indexes
        q = """
            SELECT
                index_name, column_name, column_position, descend
            FROM
                all_ind_columns
            WHERE
                table_name = '%s' order by index_name, column_position
        """ % table.upper()

        print
        curs.execute(q)
        print_cursor(curs, fmt=fmt)

        curs.close()

    def list_tables(self, fmt='pretty', show=False):
        """
        Print a simple description of the input table.
        """
        q="""
            SELECT
                owner, table_name
            FROM
                all_tables
        """

        if show:
            stderr.write(q)

        curs = self.cursor()
        curs.arraysize = _PREFETCH

        curs.execute(q) 
        print_cursor(curs,fmt=fmt)

        curs.close()

    def _process_pars(self, **keys):
        self._port=keys.get('port',_defport)
        if self._port is None: self._port=_defport

        self._dbname=keys.get('dbname',_defdb)
        if self._dbname is None: self._dbname=_defdb


    def __repr__(self):
        rep=["DESDB Connection"]
        indent=' '*4
        p=self._pwd_getter
        rep.append("%s%s@%s" % (indent,p.user,p.host))

        return '\n'.join(rep)


def cursor2dictlist(curs, lower=True):
    if curs is None:
        return None

    keys=[]
    for d in curs.description:
        key=d[0]
        if lower:
            key=key.lower()
        keys.append(key)
        
    output=[]
    try:
        for row in curs:
            tmp={}
            for i,val in enumerate(row):
                tmp[keys[i]] = val    
            output.append(tmp)
    except KeyboardInterrupt:
        curs.close()
        raise RuntimeError("Interrupt encountered")

    return output

def print_cursor(curs, fmt='csv', header='names', file=sys.stdout):
    rw=CursorWriter(fmt=fmt, file=file, header='names')
    rw.write(curs)

def write_json(obj, fmt):
    if not have_json and not have_cjson:
        raise ValueError("don't have either json or cjson libraries")

    if fmt == 'cjson':
        jstring = cjson.encode(obj)
        stdout.write(jstring)
    else:
        json.dump(obj, stdout, indent=1, separators=(',', ':'))


class CursorWriter:
    """
    The only reason for it is that, for csv, we can work row by row and save
    memory.
    """
    def __init__(self, file=sys.stdout, fmt='csv', header='names'):
        self.fmt=fmt
        self.header_type=header
        self.file=file

    def write(self, curs):
        """
        Write rows from the cursor.
        """

        if self.fmt in ['csv','space','tab']:
            self.write_csv(curs)
        elif self.fmt == 'pretty':
            self.write_pretty(curs)
        else:
            data = cursor2dictlist(curs)
            w=ObjWriter(file=self.file, 
                        fmt=self.fmt, 
                        header=self.header_type)
            w.write(data)

    def write_csv(self, curs):
        """
        Simple csv with, by default, a header
        """
        import time

        desc = curs.description
        if desc is None:
            return

        ncol = len(desc)
        if 0 == ncol:
            return

        if self.fmt=='csv':
            writer = csv.writer(self.file,dialect='excel',
                                quoting=csv.QUOTE_MINIMAL,
                                lineterminator = '\n')
        else:
            if self.fmt=='space':
                delim=' '
            elif self.fmt=='tab':
                delim='\t'
            else:
                raise ValueError("bad format type: '%s'" % self.fmt)

            writer = csv.writer(self.file,dialect='excel',
                                delimiter=delim,
                                quoting=csv.QUOTE_MINIMAL,
                                lineterminator = '\n')


        if self.header_type == 'names': 
            hdr = [d[0].lower() for d in desc]
            writer.writerow(hdr)
        else:
            pass

        nresults = 0
        try:
            for row in curs:
                writer.writerow(row)
                nresults += 1
        except KeyboardInterrupt:
            curs.close()
            raise RuntimeError("Interrupt encountered")
        return nresults

    def write_pretty(self, curs, **keys):
        try:
            self._write_pretty(curs, **keys)
        except KeyboardInterrupt:
            curs.close()
            raise RuntimeError("Interrupt encountered")

    def _write_pretty(self, curs, **keys):

        max_lens=[]
        names=[]
        for d in curs.description:
            name=d[0]
            names.append(name)
            max_lens.append( len(name) )
        nfields = len(names)

        strings=[]
        for row in curs:
            cols=[]
            for i,colval in enumerate(row):
                name=names[i]
                colstr=str(colval)
                l=len(colstr)

                if l > max_lens[i]:
                    max_lens[i]=l

                cols.append(colstr)
            strings.append(cols)
                
        # now create the formats for writing each field
        # and the separator
        separator = []
        forms = []
        for i,length in enumerate(max_lens):
            fmt='%-'+str(length)+'s'

            #pad = 2
            #if i == (nfields-1):
            #    pad=1

            #sep = '%s' % '-'*(length+pad)
            sep = '%s' % '-'*length

            forms.append(fmt)
            separator.append(sep)

        row_fmt=' | '.join(forms)
        separator='-+-'.join(separator)

        header = []
        for i in xrange(nfields): 
            name=names[i]
            cname = center_text(name,max_lens[i])
            header.append(cname)

        header=' | '.join(header)

        self.file.write(header)
        self.file.write('\n')
        self.file.write(separator)
        self.file.write('\n')

        for i,row in enumerate(strings):
            if (((i+1) % 50) == 0):
                self.file.write(separator)
                self.file.write('\n')
                self.file.write(header)
                self.file.write('\n')
                self.file.write(separator)
                self.file.write('\n')
            
            self.file.write(row_fmt % tuple(row))
            self.file.write('\n')

        self.file.flush()


class ObjWriter:
    def __init__(self, file=sys.stdout, fmt='csv', header='names'):
        self.fmt=fmt
        self.header_type=header
        self.file=file

    def write(self, data):
        """
        Write rows from the list of dictionaries.
        """

        if isinstance(data,dict) and self.fmt not in ['json','cjson','pyobj']:
            raise ValueError("Can only write a dictionary to json or pyobj")

        if self.fmt == 'csv':
            self.write_csv(data)
        elif self.fmt in ['json','cjson']:
            self.write_json(data)
        elif self.fmt == 'pretty':
            self.write_pretty(data)
        elif self.fmt == 'pyobj':
            self.write_pyobj(data)
        else:
            raise ValueError("bad format %s. Only support "
                             "csv,json,cjson,pretty,pyobj "
                             "writing for now" % self.fmt)

    def write_json(self, data):
        write_json(data, self.fmt)

    def write_pyobj(self, data):
        import pprint
        pprint.pprint(data)


    def write_csv(self, data):
        """
        Simple csv with, by default, a header
        """

        ncol = len(data[0])
        if 0 == ncol:
            return

        writer = csv.DictWriter(self.file, list(data.keys()),
                                quoting=csv.QUOTE_MINIMAL,
                                lineterminator = '\n')

        if self.header_type == 'names': 
            writer.writeheader()
        else:
            raise ValueError("Only support names as header for now")

        writer.writerows(data)

    def write_pretty(self, data, delim=' ', maxwidth=30):

        # build up a format string
        formats=[]
        separators=[]
        names=[]

        for k in data[0]:
            size_max=0
            for d in data:
                size_max=max(size_max,d[k])

            formats.append('%('+k+')'+repr(size_max)+'s')
            names.append(k)
            separators.append('-'*size_max)

        format=delim.join(formats)

        count = 0
        for row in curs:
            if ((count % 50) == 0):
                self.file.write('\n')
                self.file.write(format % tuple(names))
                self.file.write('\n')
                self.file.write(format % tuple(separators))
                self.file.write('\n')

            self.file.write(format % row)
            self.file.write('\n')

            count += 1





def get_numpy_descr(meta):
    """
    Extract a value.  Default to string for non-numeric types
    """
    ncol = meta.getColumnCount()
    descr=[]
    
    for col in xrange(1,ncol+1):
        typ = meta.getColumnTypeName(colnum)
        if 'CHAR' in typ:
            nchar=meta.getPrecision(colnum)
            dt='S%d' % nchar
        elif typ=='DATE':
            # may be OK with 21..
            dt='S23'
        elif typ == 'BINARY_DOUBLE':
            dt='f8'
        elif typ == 'BINARY_FLOAT':
            dt='f8'
        elif dtype == 'NUMBER':
            # can do better than this
            scale = meta.getScale(colnum)
            if scale > 0:
                dt='f8'
            else:
                dt='i8'
        else:
            raise ValueError("Don't know how to make fixed length "
                             "col for type '%s'" % type)

        d = (name,dt)

        descr.append(d)
    return descr

def center_text(text, width, spacer=' '):
    text = text.strip()
    space = width - len(text)
    return spacer*(space/2) + text + spacer*(space/2 + space%2)


class PasswordGetter:
    """
    Try to get username/password from different sources.

    First there are the keywords user=, password= which take precedence.

    The types to try are listed in the types= keyword as a list.
    Defaults to only trying netrc

    Allowed types are
        'netrc' or 'desdb_pass' (deprecated)

    netrc is much more general, as it can be used for any url.
    """
    def __init__(self, user=None, password=None, host=_defhost,
                 types=['netrc','desdb_pass'], **keys):

        self._host=host
        if self._host is None:
            self._host=_defhost

        self._types=types
        self._type=None
        self._password=None
        self._user=None

        if user is not None or password is not None:
            self._try_keywords(user=user, password=password)
            return
        
        for type in types:
            if self._set_username_password(type):
                self._type=type
                break

        if self._user==None:
            raise ValueError("could not determine "
                             "username/password for host '%s'" % self._host)

    @property
    def user(self):
        return self._user
    @property
    def password(self):
        return self._password
    @property
    def type(self):
        return self._type
    @property
    def host(self):
        return self._host



    def _set_username_password(self, type):
        gotit=False
        if type=='netrc':
            gotit=self._try_netrc()
        elif type=='desdb_pass':
            gotit=self._try_desdb_pass()
        else:
            raise ValueError("expected type 'netrc' or 'desdb_pass'")

        return gotit

    def _check_perms(self,fname):
        import stat
        fname=os.path.expanduser(fname)
        with open(fname) as fobj:
            prop = os.fstat(fobj.fileno())
            if prop.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
                err=("file has incorrect mode.  On UNIX use\n"
                     "    chmod go-rw %s" % fname)
                raise IOError(err)

    def _try_netrc(self):
        import netrc

        fname = os.path.join(os.environ['HOME'], ".netrc")
        if not os.path.exists(fname):
            return False

        self._check_perms(fname)

        res=netrc.netrc().authenticators(self._host)

        if res is None:
            # no authentication is needed for this host
            return False

        (user,account,passwd) = res
        self._user=user
        self._password=passwd

        return True

    def _try_keywords(self, user=None, password=None):
        if user is None or password is None:
            raise ValueError("Send either both or neither of user "
                             "password")

        self._user=user
        self._password=password
        self._type='keyword'


    def _try_desdb_pass(self):
        """
        Old deprecated way
        """
        fname=os.path.join( os.environ['HOME'], '.desdb_pass')
        if not os.path.exists(fname):
            return False

        self._check_perms(fname)

        with open(fname) as fobj:
            data=fobj.readlines()
            if len(data) != 2:
                raise ValueError("Expected first line user second line "
                                 "pass in %s" % fname)
            self._user=data[0].strip()
            self._password=data[1].strip()

        return True

def cursor2array(curs,
                 dtype=None,
                 f4_digits=_defs['f4_digits'],
                 f8_digits=_defs['f8_digits'],
                 lower=_defs['lower']):
    """
    Convert an cx_ Oracle cursor object into a NumPy array.
        
    If the dtype is not given, the description field is converted to a NumPy
    type list using the get_numpy_descr() function.

    parameters
    ----------
    dtype: numpy dtype or descr, optional
        A dtype for conversion.  If not sent it will be derived
        from the cursor.
    f4_digits, f8_digits:  int
        The number of digits to demand when converting to these types from
        number(digits,n).  The default is 6 or less for floats and 7-15 for
        double, e.g. f4_digits=6, f8_digits=15  For example if you want
        everything to be double use f4_digits=0

    EXAMPLES
        curs=conn.cursor()
        curs.execute(query)
        arr = Cursor2Array(curs)
    """
    import numpy
    if dtype is None:
        dtype=get_numpy_descr(curs.description, 
                              f4_digits=f4_digits,
                              f8_digits=f8_digits,
                              lower=lower)
    arr = numpy.fromiter(curs, dtype=dtype)
    return arr


def get_numpy_descr(odesc,
                    f4_digits=_defs['f4_digits'], 
                    f8_digits=_defs['f8_digits'],
                    lower=_defs['lower']):
    """
    Convert a list of cx_ Oracle descriptions to a list of NumPy type
    descriptions.
        
    This cx_Oracle description list is gotten from the 
    cursor description field
        cursor.description
    See get_numpy_type for the the conversion process.

    parameters
    -----------
    f4_digits, f8_digits:
        See the docs for cursor2array
    lower:
        If True then all names are converted to lower case.
        Default True
    """
    dtype=[]

    for d in odesc:
        name = d[0]
        if lower:
            name=name.lower()
        Ntype = get_numpy_type(d, f4_digits=f4_digits, f8_digits=f8_digits)
        dtype.append( (name, Ntype) )

    return dtype



def get_numpy_type(odesc,
                   f4_digits=_defs['f4_digits'],
                   f8_digits=_defs['f8_digits']):
    """
    NAME
        get_numpy_type
    PURPOSE
        Convert a cx_Oracle field description list into a NumPy type.
    USAGE
        nt = get_numpy_type(oracle_field_description, f4_digits=6, f8_digits=15)

    INPUTS
        oracle_field_description:  This is an element of the cx_Oracle
            description list.  This list is gotten from the cursor object:
                cursor.description
            An element of this list contains the following:
                (name, cx_Oracle_type, display_size, internal_size, 
                precision, scale, null_ok)
        f4_digits, f8_digits:  The number of digits to demand when converting
            to these types from number(digits,n).  The default is 6 or less
            for floats and 7-15 for double, e.g. f4_digits=6, f8_digits=15  
            For example if you want everything to be double use f4_digits=0
    Currently recognizes the following cx_Oracle types
        NATIVE_FLOAT.  This corresponds to the Oracle types 
            BINARY_FLOAT and BINARY_DOUBLE
        NUMBER with various precision, both floating point and fixed point
            NUMBER(p,s) is floating point, NUMBER(p) is integer
        STRING and character arrays of variable and fixed length. Some
            maximum length must be specified, but this is always the case
            for cx_Oracle description lists

        Be warned that Oracle supports precisions of both integers and
        floats that is far beyond the standard data types.  In these cases
        the integer size is set to 64-bit and the floating type is set to
        128 bit, but note that the 128 bit float type in numerical python is
        in practice usually limited to less precision.
    """

    err='size of %s not allowed for type %s'
    name = odesc[0]
    otype = odesc[1]
    size = odesc[3]
    digits = odesc[4]
    scale = odesc[5]
    if otype == cx_Oracle.NATIVE_FLOAT:
        # This one is easy: sizes indicate everything!
        if size == 4:
            Ntype='f4'
        elif size==8:
            Ntype='f8'
        else:
            raise ValueError(_binary_err % (size,))
    elif otype == cx_Oracle.NUMBER:
        if scale != 0:
            if digits <= f4_digits:
                Ntype='f4'
            elif digits <= f8_digits:
                Ntype='f8'
            else:
                sys.stdout.write(_flt_digits_err % (name,digits))
                Ntype='f16'
        else:
            if digits == 0:
                Ntype = 'i8'
            elif digits <= 4:
                Ntype = 'i2'
            elif digits <= 9:
                Ntype = 'i4'
            elif digits <= 18:
                Ntype= 'i8'
            else:
                sys.stdout.write(_int_digits_err % (name,digits))
                Ntype='i8'

    elif otype == cx_Oracle.STRING:
        if size <= 0:
            raise ValueError(_string_err % (name, size))
        Ntype= 'S'+str(size)
    else:
        if size <= 0:
            raise ValueError(_string_err % (name, size))
        Ntype= 'S'+str(size)
        #raise ValueError,'Unsupported data type: '+repr(otype)

    return Ntype

def array2table(arr, table_name, control_file, defs={}, create=False):
    """

    Write a numpy array with fields to an ascii file for uploading.  The ascii
    file contains in it the commands need to produce the load.  Optionally
    written is a file holding an sql query to create the required table if it
    doesn't exist.

    - Creating the table.  If the table already exists, skip this step 
        - send create=True
        - create the table using the statement written in
        {control_file}-create-table.sql

        That file will hold an sql statement with the create table statement.
        Note indexes must be added separately.

    - Loading the data into the table.
        - The control file can be sent to the sqlldr command
            sqlldr username/password control=control_file

    parameters
    ----------
    arr: numpy array
        The array, must have fields defined (e.g. a recarray)
    table_name: string
        Name of the table.
    control_file: string
        Name of the control file to write.  This file will hold
        all the control information for loading as well as the
        data itself.
    defs: dict,optional
        A dict returning a list of field defs. It is keyed by field names from
        the array.  This can be used to over-ride the defaults, e.g. to use a
        different name or to over-ride conversions for arrays.
    create: bool, optional
        If True, also write a file holding the create table statement.
        {control_file}-create-table.sql
    """
    import numpy

    arr=arr.view(numpy.ndarray)
    create_statement, alldefs = get_tabledef(arr.dtype.descr, table_name, defs=defs)

    if create:
        create_file="%s.create-table.sql" % control_file
        print 'writing create table statement',create_file
        with open(create_file,'w') as fobj:
            fobj.write(create_statement)

    names = [d[0] for d in alldefs]
    name_list=', '.join(names)

    print 'writing control file',control_file
    with open(control_file,'w') as fobj:
        top="""
load data
    infile *
    append into table
        {table_name}
    fields terminated by ","
    ( {name_list} )
begindata\n""".format(table_name=table_name,
                              name_list=name_list)
        fobj.write(top)
        _write_sqlldr_data(arr, fobj)

def _write_sqlldr_data(arr, fobj):
    try:
        import recfile
        have_recfile=True
    except:
        try:
            from esutil import recfile
            have_recfile=True
        except:
            have_recfile=False

    if have_recfile:
        rf=recfile.Recfile(fobj, mode='w+', delim=',', ignorenull=True)
        rf.Write(arr)
    else:
        raise RuntimeError("implement writing without recfile")

def get_tabledef(descr, table_name, defs={}):
    """
    Convert a numpy descriptor to oracle table creation
    statement

    array columns are converted to name_{dim1}_{dim2}...{dimn}

    parameters
    ----------
    descr: numpy type descriptor
        E.g. arr.dtype.descr
    table_name:
        Name of the table
    defs: dict,optional
        A dict returning a list of field defs. It is keyed by field names from
        the array.  This can be used to over-ride the defaults, e.g. to use a
        different name or to over-ride conversions for arrays.

    output
    ------
    The create table statement as well as the array of individual column definitions
    """

    alldefs = get_coldefs(descr, defs=defs)

    sdefs=[]
    for d in alldefs:
        sdefs.append('%s %s' % d)

    sdefs=',\n'.join(sdefs)
    statement=['create table {table_name} ('.format(table_name=table_name)]
    statement.append(sdefs)
    statement.append(') compress\n')

    statement='\n'.join(statement)
    return statement, alldefs

def get_coldefs(descr, defs={}):
    """
    Convert a numpy descriptor to a set of oracle 
    column definitions

    array columns are converted to name_{dim1}_{dim2}...{dimn}

    parameters
    ----------
    descr: numpy type descriptor
        E.g. arr.dtype.descr
    defs: dict,optional
        A dict returning a list of field defs. It is keyed by field names from
        the array.  This can be used to over-ride the defaults, e.g. to use a
        different name or to over-ride conversions for arrays.
    """

    if defs is None:
        defs={}

    alldefs=[]
    def_template='%s not null'
    for d in descr:
        name=d[0]
        ot=get_oracle_type(d[1])

        if name in defs:
            alldefs += defs[name]
        elif len(d) == 2:
            # this is a scalar column... easy!
            defi=def_template % ot
            alldefs.append( (name,defi) )
        else:
            dims=d[2]
            if not isinstance(dims,tuple):
                dims=(dims,)
            names=get_arr_colnames(name,dims)
            
            for n in names:
                defi=def_template % (ot)
                alldefs.append( (n,defi) )

    return alldefs

def get_arr_colnames(name, dims):
    """
    Get db names for an array, naming 
        name_{num1}_{num2}...
    """
    ndim=len(dims)
    if ndim==1:
        names=get_arr1_colnames(name,dims)
    elif ndim==2:
        names=get_arr2_colnames(name,dims)
    else:
        raise ValueError("only support 1 and 2 d arrays")

    return names

def get_arr1_colnames(name, dims):
    """
    Get db names for an array, naming 
        name_{num}
    """
    names=[]
    for n in xrange(1,dims[0]+1):
        names.append( '%s_%d' % (name,n) )

    return names

def get_arr2_colnames(name, dims):
    """
    Get db names for an array, naming 
        name_{num1}_{num2}
    """
    names=[]
    for n1 in xrange(1,dims[0]+1):
        for n2 in xrange(1,dims[1]+1):
            names.append( '%s_%d_%d' % (name,n1,n2) )

    return names


def get_oracle_type(nt):
    if 'f4' in nt:
        ot='binary_float'
    elif 'f8' in nt:
        ot='binary_double'
    elif 'i1' in nt or 'u1' in nt:
        ot='number(3)'
    elif 'i2' in nt or 'u2' in nt:
        ot='number(5)'
    elif 'i4' in nt:
        ot='number(10)'
    elif 'i8' in nt:
        ot='number(19)'
    elif 'u8' in nt:
        ot='number(20)'
    elif 'S' in nt:
        slen=nt[2:]
        ot='varchar2(%s)' % slen
    else:
        raise ValueError("unsupported numpy type: '%s'" % nt)

    return ot
