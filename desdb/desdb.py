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

    methods
    -------

    quick:
        Execute the query and return the results.

    quickWrite:
        Execute the query and write the results to a the
        standard output or a file.

    describe:
        Print a description of the specified table.
    """
    def __init__(self, user=None, password=None, host=_defhost,
                 port=_defport, dbname=_defdb):

        if user is not None or password is not None:
            if user is None or password is None:
                raise ValueError("Send either both or neither of user password")
        else:
            f=os.path.join( os.environ['HOME'], '.desdb_pass')
            if not os.path.exists(f):
                raise ValueError("Send user=,password= or create %s" % f)

            data=open(f).readlines()
            if len(data) != 2:
                raise ValueError("Expected first line user second line pass in %s" % f)
            user=data[0].strip()
            password=data[1].strip()

        self._host=host
        self._port=port
        self._dbname=dbname
        url = _url_template % (self._host, self._port, self._dbname)

        cx_Oracle.Connection.__init__(self,user,password,url)

    def __repr__(self):
        rep=["DESDB Connection"]
        indent=' '*4
        rep.append("%s%s@%s" % (indent,self.username,self.dsn))

        return '\n'.join(rep)

    def quick(self, query, lists=False, strings=False, array=False, show=False):
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

    def quickWrite(self, query, fmt='csv', header='names', file=sys.stdout, show=False):
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

        if show: 
            stderr.write(query)
        curs.execute(query)

        print_cursor(curs, fmt=fmt, header=header, file=file)
        curs.close()

    def describe(self, table, show=False):
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
        curs.execute(q) 
        print_cursor(curs,fmt='pretty')

        # now indexes
        q = """
            select
                index_name, column_name, column_position, descend
            from
                all_ind_columns
            where
                table_name = '%s' order by index_name, column_position
        """ % table.upper()

        curs.execute(q)
        print_cursor(curs, fmt='pretty')

        curs.close()


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

    The only reason for it is that, for csv and pretty formatting, we can work
    row by row and save memory.  The other fmts we will use the ObjWriter

    """
    def __init__(self, file=sys.stdout, fmt='csv', header='names'):
        self.fmt=fmt
        self.header_type=header
        self.file=file

    def write(self, curs):
        """
        Write rows from the cursor.
        """

        if self.fmt == 'csv':
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

        writer = csv.writer(self.file,dialect='excel',
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

    def write_pretty(self, curs, delim=' ', maxwidth=30):

        # build up a format string
        formats=[]
        separators=[]
        names=[]
        for d in curs.description:
            dsize = d[2]
            if dsize > maxwidth:
                dsize=maxwidth

            formats.append('%'+repr(dsize)+'s')
            names.append(d[0])
            separators.append('-'*dsize)

        format=delim.join(formats)

        count = 0
        try:
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
        except KeyboardInterrupt:
            curs.close()
            raise RuntimeError("Interrupt encountered")

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
                             "csv,json,cjson,pretty,pyobj writing for now" % self.fmt)

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
