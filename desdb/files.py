import copy
import os
from sys import stderr
try:
    from . import desdb
except:
    # this is usually because the oracle libraries are not installed
    pass

_release_ref_images={'Y1C2_COADD_PRERELEASE':{'g': 443103519,
                                              'r': 443105292,
                                              'i': 442546880,
                                              'z': 442540179,
                                              'Y': 442536081}}
def get_release_ref_image(release, band):
    """
    Get the reference image id for this release/band
    """
    if release not in _release_ref_images:
        raise ValueError("release '%s' not found in ref image "
                         "dict" % release)
    return _release_ref_images[release][band]

def get_release_magzp_ref(release, band):
    """
    Get the zeropoint from the reference image id for this release/band
    """
    id=get_release_ref_image(release, band)
    query="""
    select distinct(mag_zero) from zeropoint where source='GCM' and imageid=%s
    \n""" % id

    conn=desdb.Connection()
    res=conn.quick(query)

    magzp_ref = res[0]['mag_zero']
    return magzp_ref


def get_adhoc_release_runs(release):
    rmap=get_adhoc_release_map()
    fname=rmap[release]['run_file']

    with open(fname) as fobj:
            runlist=fobj.readlines()
            runlist=[run.strip() for run in runlist]

    return runlist

def get_release_runs(release, **keys):

    rmap=get_adhoc_release_map()
    if release in rmap:
        return get_adhoc_release_runs(release)

    query="""
    select distinct(run) from runtag where tag='%s'
    """ % release.upper()

    conn=desdb.Connection(**keys)
    res=conn.quick(query,**keys)
    return [r['run'] for r in res]

# my own run lists until official releases come
def get_adhoc_release_map():
    desdata=get_des_rootdir()
    rmap={}

    """
    # clusters
    sve01={}
    sve01['run_exp_file']=desdata+'/sync/2013-03-20/coadd-se-run-exp.txt'
    sve01['coadd_run_file']=desdata+'/sync/2013-03-20/coadd-runlist.txt'

    # spt east
    sve02={}
    sve02['run_exp_file']=desdata+'/sync/2013-04-03/se-run-explist-spte.txt'
    sve02['coadd_run_file']=desdata+'/sync/2013-04-03/coadd-runlist-spte.txt'
    """

    # RXJ2248
    rxj2248_coadd={}


    #rmap['sve01']=sve01
    #rmap['sve02']=sve02
    rmap['RXJ2248-coadd-2013-06-19'] = {}
    rmap['RXJ2248-coadd-2013-06-19']['run_file'] = \
            desdata+'/sync/2013-06-19/coadd-runlist-RXJ2248-2013-06-19.txt'
    #rmap['sve01-se']['run_file'] = desdata+'/sync/2013-03-20/coadd-se-run-exp.txt'
    #rmap['sve01-me']['run-file'] = desdata+'/sync/2013-03-20/coadd-runlist.txt'
    #rmap['spte-se']['run-file'] = desdata+'/sync/2013-04-03/se-run-explist-spte.txt'
    #rmap['spte-me']['run-file'] = desdata+'/sync/2013-04-03/coadd-runlist-spte.txt'
    return rmap

SKIP_CCD=61

def get_default_fs():
    return os.environ.get('DES_DEFAULT_FS','nfs')

def get_des_rootdir(**keys):
    default_fs=get_default_fs()
    fs=keys.get('fs',default_fs)
    if fs == 'nfs':
        return get_nfs_rootdir()
    elif fs == 'hdfs':
        return get_hdfs_rootdir()
    elif fs == 'net':
        return get_net_rootdir()
    else:
        raise ValueError("fs should be 'nfs' or 'net' or 'hdfs'")

def get_default_des_project():
    if 'DESPROJ' not in os.environ:
        raise ValueError("DESPROJ environment variable is not set")
    return os.environ['DESPROJ']

def get_nfs_rootdir():
    if 'DESDATA' not in os.environ:
        raise ValueError("The DESDATA environment variable is not set")
    return os.environ['DESDATA']

def get_hdfs_rootdir():
    return 'hdfs:///user/esheldon/DES'

def get_net_rootdir():
    if 'DESREMOTE' not in os.environ:
        raise ValueError("The DESREMOTE environment variable is not set")
    return os.environ['DESREMOTE']

def get_scratch_dir():
    if 'DES_SCRATCH' not in os.environ:
        raise ValueError("The DES_SCRATCH environment variable is not set")
    return os.environ['DES_SCRATCH']


def get_dir(type, **keys):
    df=DESFiles(**keys)
    return df.dir(type, **keys)
def get_url(type, **keys):
    df=DESFiles(**keys)
    return df.url(type, **keys)

get_name=get_url
get_path=get_url

def get_coadd_info_by_runlist(runlist, band):
    """
    Band is a scalar
    """

    flist=[]
    for run in runlist:
        coadd=Coadd(coadd_run=run, band=band)
        coadd.load()
        flist.append( coadd )
    return flist

def _get_coadd_info_cache_fname(release, band):
    desdata=get_des_rootdir()
    dir=os.path.join(desdata,'users','esheldon','query-cache')
    fname='query-%s-%s.json' % (release, band)
    fname=os.path.join(dir,fname)
    return fname

def _write_coadd_info_cache(release, band, data):
    import json
    fname=_get_coadd_info_cache_fname(release, band)
    d=os.path.dirname(fname)
    if not os.path.exists(d):
        os.makedirs(d)

    print 'writing cache:',fname
    with open(fname,'w') as fobj:
        json.dump(data, fobj, indent=1, separators=(',', ':'))
 
def _read_coadd_info_cache(release, band):
    import json
    fname=_get_coadd_info_cache_fname(release, band)
    if os.path.exists(fname):
        print 'loading cache:',fname
        with open(fname) as fobj:
            data=json.load(fobj)
    else:
        data=None
    return data

def get_coadd_info_by_release(release, band):

    rmap=get_adhoc_release_map()
    if release in rmap:
        data=_read_coadd_info_cache(release, band)

        if data is None:

            runlist=get_release_runs(release)
            data = get_coadd_info_by_runlist(runlist, band)
            _write_coadd_info_cache(release, band, data)

        return data
    else:
        data=_read_coadd_info_cache(release, band)
        if data is None:
            # all bands are under the same run for coadd
            query="""
            select
                distinct(run)
            from
                runtag
            where
                tag='%s'
            \n""" % release

            conn=desdb.Connection()
            curs = conn.cursor()
            curs.execute(query)

            runlist = [r[0] for r in curs]

            curs.close()

            data = get_coadd_info_by_runlist(runlist, band)
            _write_coadd_info_cache(release, band, data)

        return data

def get_expnames_by_release(release, band, show=False,
                            user=None,password=None):
    """
    doesn't work any more
    This is usually much faster then the get_red_info query
    """
    # note removing 0 dec stuff because there are dups
    query="""
    select
        distinct(file_exposure_name) as expname
    from
        %(release)s_files
    where
        filetype='red'
        and band='%(band)s'
        and file_exposure_name not like '%%-0-%(band)s%%'
    """ % {'release':release,'band':band}

    conn=desdb.Connection(user=user,password=password)
    curs = conn.cursor()
    curs.execute(query)

    expnames = [r[0] for r in curs]

    curs.close()
    return expnames

def get_red_info_by_runlist(runlist, explist=None,
                            user=None,
                            password=None,
                            host=None):
    """
    runlist and explist are paired
    """

    conn=desdb.Connection(user=user,password=password,host=host)

    desdata=get_des_rootdir()
    dlist=[]

    if explist is not None:
        for run,expname in zip(runlist,explist):

            query="""
            select
                '%(desdata)s/' || loc.project || '/red/' || image.run || '/red/' || loc.exposurename || '/' || image.imagename || '.fz' as image_url,
                loc.exposurename as expname,
                loc.band,
                image.ccd,
                image.id as image_id,
                image.run as red_run
            from
                image, location loc
            where
                image.run = '%(run)s'
                and loc.exposurename = '%(expname)s'
                and loc.id=image.id
                and image.imagetype='red'
                and image.ccd != 61\n"""

            query=query % {'run':run,
                           'expname':expname,
                           'desdata':desdata}

            data=conn.quick(query)

            dlist += data
    else:
        for run in runlist:

            query="""
            select
                '%(desdata)s/' || loc.project || '/red/' || image.run || '/red/' || loc.exposurename || '/' || image.imagename || '.fz' as image_url,
                loc.exposurename as expname,
                loc.band,
                image.ccd,
                image.id as image_id,
                image.run as red_run
            from
                image, location loc
            where
                image.run = '%(run)s'
                and loc.id=image.id
                and image.imagetype='red'
                and image.ccd != 61\n"""

            query=query % {'run':run, 'desdata':desdata}

            data=conn.quick(query)

            dlist += data

    return dlist


def get_red_info_by_runlist_old(runlist, 
                            user=None,
                            password=None,
                            host=None,
                            desdata=None,
                            show=True,
                            doprint=False,
                            fmt='json',
                            asdict=False):
    """
    Get all image and cat info for the input list of runs
    """

    conn=desdb.Connection(user=user,password=password,host=host)

    runcsv = ','.join(runlist)
    runcsv = ["'%s'" % r for r in runlist]
    runcsv = ','.join(runcsv)

    query="""
    select
        '%(desdata)s/' || loc.project || '/red/' || image.run || '/red/' || loc.exposurename || '/' || image.imagename || '.fz' as image_url,
        loc.exposurename as expname,
        loc.band,
        image.ccd,
        image.id as image_id
    from
        image, location loc
    where
        image.run in (%(runcsv)s)
        and loc.id=image.id
        and image.imagetype='red'
        and image.ccd != 61\n"""

    desdata=get_des_rootdir()
    query=query % {'runcsv':runcsv,
                   'desdata':desdata}

    if doprint:
        conn.quickWrite(query,fmt=fmt,show=show)
    else:
        data=conn.quick(query,show=show)
        return data

def _read_runexp(fname):
    runlist=[]
    explist=[]
    with open(fname) as fobj:
        for line in fobj:
            ls=line.split()
            runlist.append(ls[0])
            explist.append(ls[1])
    return runlist,explist


def get_red_info_by_release(release, band=None, 
                            user=None,
                            password=None,
                            host=None,
                            show=True,
                            doprint=False,
                            fmt='json',
                            asdict=False):

    rmap=get_adhoc_release_map()
    if release in rmap:

        fname=rmap[release]['run_exp_file']
        runlist,explist=_read_runexp(fname)
        return get_red_info_by_runlist(runlist, explist=explist,
                                       user=user,
                                       password=password,
                                       host=host,
                                       asdict=asdict)
    else:
        runs=get_release_runs(release)
        dlist = get_red_info_by_runlist(runs)
        
        if band is not None:
            dlist_new=[d for d in dlist if d['band']==band]
            dlist=dlist_new
        return dlist

    return

    desdata=get_des_rootdir()
    net_rootdir=get_des_rootdir(fs='net')

    # note removing 0 dec stuff because there are dups
    query="""
    select
        im.project,
        im.file_exposure_name as expname,
        im.band,
        im.ccd,
        im.id as image_id,
        '%(desdata)s/'    || im.project || '/' || im.path as image_url,
        '%(netroot)s/' || im.project || '/' || im.path as image_url_remote,
        cat.id as cat_id,
        '%(desdata)s/'    || im.project || '/' || cat.path as cat_url,
        '%(netroot)s/' || im.project || '/' || cat.path as cat_url_remote
    from
        %(release)s_files cat,
        %(release)s_files im
    where
        cat.filetype='red_cat'
        and cat.band='%(band)s'
        and cat.catalog_parentid = im.id
        and cat.file_exposure_name not like '%%-0-%(band)s%%'
    order by 
        cat_id\n"""

    query=query % {'netroot':net_rootdir,
                   'release':release,
                   'band':band}

    conn=desdb.Connection(user=user,password=password,host=host)
    if doprint:
        conn.quickWrite(query,fmt=fmt,show=show)
    else:
        data=conn.quick(query,show=show)
        return data

def get_red_info_release_byexp(release, band, 
                               user=None,password=None,
                               show=True,
                               doprint=False, fmt='json'):

    infolist = get_red_info_by_release(release, band, 
                                       user=user,password=password,
                                       show=show)

    d={}
    for info in infolist:
        expname=info['expname']
        el=d.get(expname,None)
        if el is None:
            d[expname] = [info]
        else:
            # should modify in place
            el.append(info)
            if len(el) > 62:
                pprint.pprint(el)
                raise ValueError("%s grown beyond 62, why?" % expname)
    return d

class Red(dict):
    def __init__(self, 
                 id=None, 
                 expname=None,
                 ccd=None,
                 release=None,
                 verbose=False, 
                 user=None, password=None,
                 conn=None):
 
        """
        id is the red image id
        """
        if id is not None:
            self.method='id'
        elif expname is not None and release is not None:
            self.method='release-exp-ccd'
        else:
            raise ValueError("send either id= or (expname= and ccd= and release=). "
                             "-- Should add run=,expname=")

        self['image_id']=id
        self['cat_id'] = None
        self['expname'] = expname
        self['ccd'] = ccd
        self['release']=release

        self.verbose=verbose

        if conn is None:
            self.conn=desdb.Connection(user=user,password=password)
        else:
            self.conn=conn

    def load(self):

        if self.method == 'id':
            self._get_info_by_id()
        else:
            self._get_info_by_release()

        df=DESFiles()
        self['image_url'] = df.url('red_image', 
                                   run=self['image_run'], 
                                   expname=self['expname'],
                                   ccd=self['ccd'])
        self['cat_url'] = df.url('red_cat', 
                                 run=self['cat_run'], 
                                 expname=self['expname'],
                                 ccd=self['ccd'])


    def _get_info_by_id(self):
        query="""
        select
            cat.id as cat_id,
            im.run as image_run,
            cat.run as cat_run,
            im.exposurename as expname,
            im.ccd as ccd,
            im.band
        from
            location im,
            catalog cat
        where
            cat.catalogtype='red_cat'
            and cat.parentid = im.id
            and im.id = %(id)s\n""" % {'id':self['image_id']}

        res=self.conn.quick(query,show=self.verbose)

        if len(res) > 1:
            raise ValueError("Expected a single result, found %d")

        for key in res[0]:
            self[key] = res[0][key]


    def _get_info_by_release(self):
        query="""
        select
            im.id as image_id,
            cat.id as cat_id,
            im.run as image_run,
            cat.run as cat_run,
            im.band
        from
            %(release)s_files cat,
            %(release)s_files im
        where
            cat.filetype='red_cat'
            and cat.catalog_parentid = im.id
            and cat.file_exposure_name = '%(expname)s'
            and cat.ccd = %(ccd)s\n""" % {'expname':self['expname'],
                                          'ccd':self['ccd'],
                                          'release':self['release']}

        res=self.conn.quick(query,show=self.verbose)
        if len(res) != 1:
            raise ValueError("Expected a single result, found %d" % len(res))

        for key in res[0]:
            self[key] = res[0][key]




class Coadd(dict):
    def __init__(self, 
                 id=None, 
                 coadd_run=None,
                 band=None, 
                 fs=None,
                 verbose=False, 
                 user=None,
                 password=None,
                 host=None,
                 conn=None):
        """
        Construct either with
            c=Coadd(id=)
        or
            c=Coadd(coadd_run=, band=)

        Sending a connection can speed things up greatly.
        """
        if id is not None:
            self.method='id'
        elif coadd_run is not None and band is not None:
            self.method='runband'
        else:
            raise ValueError("Send id= or (coadd_run=,band=)")

        self.verbose=verbose
        if not fs:
            fs=get_default_fs()
        self.fs=fs

        self.image_id=id
        self.coadd_run=coadd_run
        self.band=band

        if conn is None:
            self.conn=desdb.Connection(user=user,password=password,host=host)
        else:
            self.conn=conn

    def load(self, srclist=False):

        if self.method == 'id':
            self._get_info_by_id()
        elif self.method == 'runband':
            self._get_info_by_runband()

        df=DESFiles(fs=self.fs)
        self['image_url'] = df.url('coadd_image', 
                                   coadd_run=self['coadd_run'], 
                                   tilename=self['tilename'], 
                                   band=self['band'])
        self['cat_url'] = df.url('coadd_cat', 
                                 coadd_run=self['coadd_run'], 
                                 tilename=self['tilename'], 
                                 band=self['band'])

        if srclist:
            self._load_srclist()
        

    def _get_info_by_runband(self):
        query="""
        select
            im.id as image_id,
            im.band,
            im.run as coadd_run,
            im.sexmgzpt as magzp,
            cat.id as cat_id,
            im.tilename
        from
            coadd im,
            catalog cat 
        where
            cat.catalogtype='coadd_cat'
            and cat.parentid = im.id
            and im.run = '%(run)s'
            and im.band = '%(band)s'\n""" % {'run':self.coadd_run,
                                             'band':self.band}

        res=self.conn.quick(query,show=self.verbose)

        if len(res) > 1:
            vals=(len(res),self['coadd_run'],self['band'])
            raise ValueError("got %d entries for coadd_run=%s band=%s" % vals)
        for key in res[0]:
            self[key] = res[0][key]

    def _get_info_by_id(self):
        query="""
        select
            cat.id as cat_id,
            im.run as coadd_run,
            im.id as image_id,
            im.sexmgzpt as magzp,
            im.band,
            im.tilename
        from
            coadd im,
            catalog cat
        where
            cat.catalogtype='coadd_cat'
            and cat.parentid = im.id
            and im.id = %(id)s\n""" % {'id':self.image_id}

        res=self.conn.quick(query,show=self.verbose)

        if len(res) > 1:
            raise ValueError("Expected a single result, found %d")

        for key in res[0]:
            self[key] = res[0][key]


    def _load_srclist(self):
        """
        This new query works because the new system is more sane
            - guaranteed number of levels between coadd and SE image
            - single coadd run for all bands
        
        Thanks to Bob Armstrong for the new query

        Note for non-psf homogenized coadds, will use one less level.
        See Bob's email.
        """

        query="""
        SELECT
            magzp,e.id
        FROM
            coadd_src,coadd,image c,image d,image e
        WHERE
            coadd.band='{band}'
            and coadd_src.coadd_imageid=coadd.id
            and coadd.run='{coadd_run}'
            and c.id=coadd_src.src_imageid
            and c.parentid=d.id
            and d.parentid=e.id\n"""

        query=query.format(band=self['band'],
                           coadd_run=self['coadd_run'])

        res = self.conn.quick(query, show=self.verbose)
        idlist=[]
        zpdict={}
        for d in res:
            tid=d['id']
            idlist.append(str(tid))
            zpdict[tid] = d['magzp']

        idcsv = ', '.join(idlist)

        query="""
        select 
            id,run,exposurename as expname,ccd
        from 
            location 
        where 
            id in (%(idcsv)s) 
        order by id\n""" % {'idcsv':idcsv}

        res = self.conn.quick(query)
        if len(res) != len(zpdict):
            raise ValueError("expected %d sources but "
                             "got %d" % (len(zpdict),len(res)))

        df=DESFiles(fs=self.fs)
        srclist=[]
        for r in res:
            r['magzp'] = zpdict[r['id']]
            for type in ['image','bkg','seg','cat']:
                ftype='red_%s' % type
                url=df.url(ftype,
                           run=r['run'],
                           expname=r['expname'],
                           ccd=r['ccd'])
                r[ftype] = url
            srclist.append(r)

        self.srclist=srclist



class DESFiles:
    """
    Generate file urls/paths from filetype, run, etc.

    The returned name is a local path or web url.  The generic name "url" is
    used for both.

    parameters
    ----------
    fs: string, optional
        The file system.  Default is DES_DEFAULT_FS
    """
    def __init__(self, **keys):
        if 'fs' not in keys:
            fs=get_default_fs()
        else:
            fs=keys['fs']
        self.fs = fs
        self._root=get_des_rootdir(fs=self.fs)

    def root(self):
        return self._root
    
    def dir(self, type=None, **keys):
        """
        Get the DES directory for the input file type

        parameters
        ----------
        type: string
            The directory type, e.g. 'red_run'.  See the _fs
            dict.  If None, the root directory is returned
        run: string
            The run id
        expname:
            Exposure name
            Can also be built up by sending keywords
                pointing,band and visit
        """
        if type is None:
            return self.root()

        if type not in _fs:
            raise ValueError("Unsupported path type '%s'" % type)
        
        if self.fs == 'net':
            url = _fs[type]['remote_dir']
        else:
            url = _fs[type]['dir']

        url = self._expand_desvars(url, **keys)
        return url

    def url(self, type=None, **keys):
        """
        Get the URL (local or remote) for the file type.

        possible parameters (there could be others required)
        -------------------
        type: string
            The file type, e.g. 'red_image'.  See the _fs dict.
            dict.  If None, the root directory is returned
        run: string
            The run id

        The rest of the URL is built up from some combination
        of the following that depends on the file type

        expname:
            Exposure name
            Can also be built up by sending keywords
                pointing, band and visit
        ccd: string/number
            The ccd number
        band: string
            The band, e.g. 'i'
        tilename: string
            Tilename for coadds
        """
        if type is None:
            return self.root()

        url = self.dir(type, **keys)
        if 'name' in _fs[type]:
            url = os.path.join(url, _fs[type]['name'])
        url = self._expand_desvars(url, **keys)
        return url
    name=url

    def _expand_desvars(self, url, **keys):
        keys['fs'] = self.fs
        return expand_desvars(url, **keys)


# notes 
#   - .fz might not always hold
#   - EXPNAME can also be built from POINTING-BAND-VISIT
_fs={}
_fs['red_run']   = {'remote_dir':'$DESREMOTE/$DESPROJ/red/$RUN/red',
                    'dir':         '$DESDATA/$DESPROJ/red/$RUN/red'}

_fs['red_exp']   = {'remote_dir':'$DESREMOTE/$DESPROJ/red/$RUN/red/$EXPNAME',
                    'dir':       '$DESDATA/$DESPROJ/red/$RUN/red/$EXPNAME'}


_fs['red_image'] = {'remote_dir':_fs['red_exp']['remote_dir'],
                    'dir':       _fs['red_exp']['dir'], 
                    'name':'$EXPNAME_$CCD.fits.fz'}

_fs['red_cat']   = {'remote_dir':_fs['red_exp']['remote_dir'],
                    'dir':       _fs['red_exp']['dir'], 
                    'name':'$EXPNAME_$CCD_cat.fits'}
_fs['red_bkg']   = {'remote_dir':_fs['red_exp']['remote_dir'],
                    'dir':       _fs['red_exp']['dir'], 
                    'name':'$EXPNAME_$CCD_bkg.fits.fz'}
_fs['red_seg']   = {'remote_dir':_fs['red_exp']['remote_dir'],
                    'dir':       _fs['red_exp']['dir'], 
                    'name':'$EXPNAME_$CCD_seg.fits.gz'}



_fs['coadd_run'] = {'remote_dir': '$DESREMOTE/$DESPROJ/coadd/$COADD_RUN/coadd',
                    'dir':        '$DESDATA/$DESPROJ/coadd/$COADD_RUN/coadd'}
_fs['coadd_image'] = {'remote_dir': _fs['coadd_run']['remote_dir'],
                      'dir':        _fs['coadd_run']['dir'], 
                      'name':       '$TILENAME_$BAND.fits.fz'}
_fs['coadd_cat']   = {'remote_dir': _fs['coadd_run']['remote_dir'],
                      'dir':_fs['coadd_run']['dir'], 
                      'name':'$TILENAME_$BAND_cat.fits'}
_fs['coadd_seg']   = {'remote_dir': _fs['coadd_run']['remote_dir'],
                      'dir':_fs['coadd_run']['dir'], 
                      'name':'$TILENAME_$BAND_seg.fits.gz'}

# Multi Epoch Data Structure files
# should have a run based system?  The input coadd run set
# will be changing constantly

_meds_dir='$DESDATA/meds/$MEDSCONF/$COADD_RUN'
_meds_script_dir='$DESDATA/meds/$MEDSCONF/scripts/$COADD_RUN'
_fs['meds'] = {'dir': _meds_dir, 'name': '$TILENAME-$BAND-meds-$MEDSCONF.fits.fz'}
_fs['meds_input'] = {'dir': _meds_dir,
                     'name':'$TILENAME-$BAND-meds-input-$MEDSCONF.dat'}
_fs['meds_srclist'] = {'dir': _meds_dir,
                       'name':'$TILENAME-$BAND-meds-srclist-$MEDSCONF.dat'}
_fs['meds_status'] = {'dir':_meds_dir,
                      'name':'$TILENAME-$BAND-meds-status-$MEDSCONF.yaml'}

_fs['meds_script'] = {'dir':_meds_script_dir,
                      'name':'$TILENAME-$BAND-meds.sh'}
_fs['meds_log'] = {'dir':_meds_script_dir,
                   'name':'$TILENAME-$BAND-meds.pbs.log'}
_fs['meds_pbs'] = {'dir':_meds_script_dir,
                   'name':'$TILENAME-$BAND-meds.pbs'}


#
# outputs from any weak lensing pipeline
#

# se exp names have underscores so we use underscores
_fs['wlpipe'] = {'dir': '$DESDATA/wlpipe'}
_fs['wlpipe_run'] = {'dir': _fs['wlpipe']['dir']+'/$RUN'}

#_fs['wlpipe_pbs'] = {'dir': _fs['wlpipe_run']['dir']+'/pbs'}
_fs['wlpipe_scratch'] = {'dir': '$TMPDIR/DES/wlpipe'}
_fs['wlpipe_scratch_run'] = {'dir': _fs['wlpipe_scratch']['dir']+'/$RUN'}
_fs['wlpipe_pbs'] = {'dir': _fs['wlpipe_scratch_run']['dir']+'/pbs'}

_fs['wlpipe_flists'] = {'dir': _fs['wlpipe_run']['dir']+'/flists'}
_fs['wlpipe_flist_red'] = {'dir': _fs['wlpipe_flists']['dir'],
                           'name':'$RUN_red_info.json'}

# SE files by exposure name
_fs['wlpipe_exp'] = {'dir': _fs['wlpipe_run']['dir']+'/$EXPNAME'}

# generic, for user use
_fs['wlpipe_se_generic'] = {'dir': _fs['wlpipe_exp']['dir'],
                            'name': '$RUN_$EXPNAME_$CCD_$FILETYPE.$EXT'}


# required
# meta has inputs, outputs, other metadata
_fs['wlpipe_se_meta'] = {'dir': _fs['wlpipe_exp']['dir'],
                         'name': '$RUN_$EXPNAME_$CCD_meta.json'}
_fs['wlpipe_se_status'] = {'dir': _fs['wlpipe_exp']['dir'],
                           'name': '$RUN_$EXPNAME_$CCD_status.txt'}

# scripts are also pbs scripts
_fs['wlpipe_se_script'] = \
    {'dir': _fs['wlpipe_pbs']['dir']+'/byexp/$EXPNAME',
     'name': '$EXPNAME_$CCD_script.pbs'}
_fs['wlpipe_se_check'] = \
    {'dir': _fs['wlpipe_pbs']['dir']+'/byexp/$EXPNAME',
     'name': '$EXPNAME_$CCD_check.pbs'}
_fs['wlpipe_se_log'] = \
    {'dir': _fs['wlpipe_pbs']['dir']+'/byexp/$EXPNAME',
     'name': '$EXPNAME_$CCD_script.pbs.log'}


# ME files by tilename and band
# tile names have dashes so we use dashes
_fs['wlpipe_tile'] = {'dir': _fs['wlpipe_run']['dir']+'/$TILENAME-$BAND'}
_fs['wlpipe_scratch_tile'] = {'dir': _fs['wlpipe_scratch_run']['dir']+'/$TILENAME-$BAND'}

# non-split versions
_fs['wlpipe_me_generic'] = {'dir': _fs['wlpipe_tile']['dir'],
                            'name': '$RUN-$TILENAME-$BAND-$FILETYPE.$EXT'}

#_fs['wlpipe_me_meta'] = {'dir': _fs['wlpipe_tile']['dir'],
#                         'name': '$RUN-$TILENAME-$BAND-meta.json'}
_fs['wlpipe_me_meta'] = {'dir': _fs['wlpipe_scratch_tile']['dir'],
                         'name': '$RUN-$TILENAME-$BAND-meta.json'}
_fs['wlpipe_me_status'] = {'dir': _fs['wlpipe_tile']['dir'],
                           'name': '$RUN-$TILENAME-$BAND-status.txt'}

# ME split versions
_fs['wlpipe_me_split'] = \
    {'dir': _fs['wlpipe_tile']['dir'],
     'name': '$RUN-$TILENAME-$BAND-$START-$END-$FILETYPE.$EXT'}

_fs['wlpipe_me_meta_split'] = \
    {'dir': _fs['wlpipe_scratch_tile']['dir'],
     'name': '$RUN-$TILENAME-$BAND-$START-$END-meta.json'}
_fs['wlpipe_me_status_split'] = \
    {'dir': _fs['wlpipe_tile']['dir'],
     'name': '$RUN-$TILENAME-$BAND-$START-$END-status.txt'}

# these names are independent of me or se
_fs['wlpipe_master_script'] =  {'dir': _fs['wlpipe_pbs']['dir'], 'name': 'master.sh'}
_fs['wlpipe_commands'] = {'dir': _fs['wlpipe_pbs']['dir'], 'name': 'commands.txt'}



_fs['wlpipe_me_tile_commands'] = {'dir': _fs['wlpipe_pbs']['dir'],
                                  'name': '$TILENAME-commands.txt'}
_fs['wlpipe_me_tile_minions'] = {'dir': _fs['wlpipe_pbs']['dir'],
                                 'name': '$TILENAME-minions.pbs'}






_fs['wlpipe_me_script_split'] = \
    {'dir': _fs['wlpipe_pbs']['dir']+'/bytile/$TILENAME-$BAND',
     'name': '$TILENAME-$BAND-$START-$END-script.pbs'}
_fs['wlpipe_me_check_split'] = \
    {'dir': _fs['wlpipe_pbs']['dir']+'/bytile/$TILENAME-$BAND',
     'name': '$TILENAME-$BAND-$START-$END-check.pbs'}
_fs['wlpipe_me_log_split'] = \
    {'dir': _fs['wlpipe_pbs']['dir']+'/bytile/$TILENAME-$BAND',
     'name': '$TILENAME-$BAND-$START-$END-script.pbs.log'}



_fs['wlpipe_minions'] = {'dir': _fs['wlpipe_pbs']['dir'], 'name': 'minions.pbs'}
_fs['wlpipe_minions_check'] = {'dir': _fs['wlpipe_pbs']['dir'],
                               'name': 'check-minions.pbs'}
_fs['wlpipe_check_reduce'] = {'dir': _fs['wlpipe_pbs']['dir'],
                              'name': 'reduce-check.py'}

_fs['wlpipe_collated'] = {'dir':_fs['wlpipe_run']['dir']+'/collated'}
_fs['wlpipe_collated_goodlist'] = {'dir':_fs['wlpipe_collated']['dir'],
                                   'name':'$RUN-goodlist.json'}
_fs['wlpipe_collated_badlist'] = {'dir':_fs['wlpipe_collated']['dir'],
                                  'name':'$RUN-badlist.json'}



def expand_desvars(string_in, **keys):

    string=string_in
    root=get_des_rootdir(**keys)
    root_remote=get_des_rootdir(fs='net')


    if string.find('$DESDATA') != -1:
        string = string.replace('$DESDATA', root)

    if string.find('$TMPDIR') != -1:
        tmpdir=os.environ['TMPDIR']
        string = string.replace('$TMPDIR', tmpdir)

    if string.find('$DESREMOTE') != -1:
        string = string.replace('$DESREMOTE', root_remote)

    if string.find('$DESPROJ') != -1:
        project=keys.get('project', None)
        if project is None:
            project=get_default_des_project()

        string = string.replace('$DESPROJ', str(project))


    if string.find('$RUN') != -1:
        run=keys.get('run', None)
        if run is None:
            raise ValueError("run keyword must be sent: '%s'" % string_in)
        string = string.replace('$RUN', str(run))

    if string.find('$COADD_RUN') != -1:
        coadd_run=keys.get('coadd_run', None)
        if coadd_run is None:
            raise ValueError("coadd_run keyword must "
                             "be sent: '%s'" % string_in)
        string = string.replace('$COADD_RUN', str(coadd_run))



    if string.find('$EXPNAME') != -1:
        expname=keys.get('expname', None)
        if expname is None:
            if 'pointing' in keys and 'band' in keys and 'visit' in keys:
                expname='%s-%s-%s'
                expname=expname% (keys['pointing'],keys['band'],keys['visit'])
        if expname is None:
            raise ValueError("expname keyword or pointing,band,visit keywords "
                             "must be sent: '%s'" % string_in)

        string = string.replace('$EXPNAME', str(expname))

    if string.find('$CCD') != -1:
        ccd=keys.get('ccd', None)
        if ccd is None:
            raise ValueError("ccd keyword must be sent: '%s'" % string_in)
        ccd=int(ccd)

        string = string.replace('$CCD', '%02i' % ccd)

    if string.find('$BAND') != -1:
        band=keys.get('band', None)
        if band is None:
            raise ValueError("band keyword must be sent: '%s'" % string_in)

        if isinstance(band,list):
            bstr=''.join(band)
        else:
            bstr=str(band)
        string = string.replace('$BAND', bstr)


    if string.find('$TILENAME') != -1:
        tilename=keys.get('tilename', None)
        if tilename is None:
            raise ValueError("tilename keyword must be sent: '%s'" % string_in)
        string = string.replace('$TILENAME', str(tilename))

    if string.find('$MEDSCONF') != -1:
        medsconf=keys.get('medsconf', None)
        if medsconf is None:
            raise ValueError("medsconf keyword must be sent: '%s'" % string_in)
        string = string.replace('$MEDSCONF', str(medsconf))

    if string.find('$FILETYPE') != -1:
        filetype=keys.get('filetype', None)
        if filetype is None:
            raise ValueError("filetype keyword must be sent: '%s'" % string_in)
        string = string.replace('$FILETYPE', str(filetype))

    if string.find('$START') != -1:
        start=keys.get('start', None)
        if start is None:
            raise ValueError("start keyword must be sent: '%s'" % string_in)
        string = string.replace('$START', str(start))
    if string.find('$END') != -1:
        end=keys.get('end', None)
        if end is None:
            raise ValueError("end keyword must be sent: '%s'" % string_in)
        string = string.replace('$END', str(end))




    if string.find('$EXT') != -1:
        ext=keys.get('ext', None)
        if ext is None:
            raise ValueError("ext keyword must be sent: '%s'" % string_in)
        string = string.replace('$EXT', str(ext))




    # see if there are any leftover un-expanded variables.  If so
    # raise an exception
    if string.find('$') != -1:
        raise ValueError("There were unexpanded variables: '%s'" % string)

    return string

