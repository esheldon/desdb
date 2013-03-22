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
_defdb = 'desoper'

_release_map={'dc6b':'dr012', 'dr012':'dr012'}

_PREFETCH=10000

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
              show=False):
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
        show: bool, optional
            If True, print the query to stderr
        """

        curs=self.cursor()

        # pre-fetch
        curs.arraysize = _PREFETCH

        if show: 
            stderr.write(query)
        curs.execute(query)

        if lists:
            res=[]
            try:
                for r in curs:
                    res.append(r)
            except KeyboardInterrupt:
                curs.close()
                raise RuntimeError("Interrupt encountered")

        elif array:
            raise ValueError("Implement array conversion")
        else:
            res = cursor2dictlist(curs)

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

        print_cursor(curs, fmt=fmt, header=header, file=file)
        curs.close()

    def describe(self, table, fmt='pretty', show=False):
        """
        Print a simple description of the input table.
        """
        q="""
            SELECT
                column_name, 
                CAST(data_type as VARCHAR2(15)) as type, 
                CAST(data_length as VARCHAR(6)) as length, 
                CAST(data_precision as VARCHAR(9)) as precision, 
                CAST(data_scale as VARCHAR(5)) as scale, 
                CAST(nullable as VARCHAR(8)) as nullable
            FROM
                all_tab_columns
            WHERE
                table_name = '%s'
                AND column_name <> 'TNAME'
                AND column_name <> 'CREATOR'
                AND column_name <> 'TABLETYPE'
                AND column_name <> 'REMARKS'
            ORDER BY 
                column_id
        """

        q = q % (table.upper(),)

        if show:
            stderr.write(q)

        curs = self.cursor()
        curs.arraysize = _PREFETCH

        curs.execute(q) 
        print_cursor(curs,fmt=fmt)

        # now indexes
        q = """
            select
                index_name, column_name, column_position, descend
            from
                all_ind_columns
            where
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
            fmt='%'+str(length)+'s'

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
                 types=['netrc','desdb_pass']):

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




