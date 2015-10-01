import copy
import os
from sys import stderr
from pprint import pprint

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



def get_sql_release_list(release):
    """
    For use in an sql query
    """
    if isinstance(release,basestring):
        release=[release]

    return ','.join( ["'%s'" % r.upper() for r in release] )

def get_coadd_run_bands(run, conn=None, **keys):
    query="""
    select
        distinct band
    from
        coadd
    where
        run='%s'
    """ % run

    if conn is None:
        conn=desdb.Connection(**keys)

    res=conn.quick(query,**keys)

    return [r['band'] for r in res]

def get_testbed_runs(runconfig):
    import deswl

    testbed=runconfig["testbed"]
    print "using testbed:",testbed

    testbed_name=testbed["name"]

    release=testbed["release"]
    allruns=get_release_runs(release, withbands=runconfig['withbands'])

    keeptiles=deswl.desmeds.testbeds[testbed_name]

    runs=[]
    for tile in keeptiles:
        for run in allruns:
            if tile in run:
                runs.append(run)
                continue

    print "kept %d/%d runs" % (len(runs), len(allruns))
    return runs

def get_release_runs(release, **keys):
    rl = get_sql_release_list(release)

    withbands=keys.get('withbands',None)
    if withbands:
        band_count = len(withbands)
        bands_s = ",".join("'%s'"%b for b in withbands)
        query = """
        select distinct(rt.run)
            from runtag rt
        where rt.tag in (%s)
        and
        (select count(c.band) from coadd c where run=rt.run and c.band in (%s))=%d
        """ % (rl, bands_s, band_count)
    else:
        query="""
        select distinct(run) from runtag where tag in (%s)
        """ % rl


    conn=desdb.Connection(**keys)
    res=conn.quick(query,**keys)
    runs = [r['run'] for r in res]
    conn.close()
    return runs

# these are sub-chunks we like to work with, but which are not defined
# in the database
def _get_adhoc_release_tiles():
    release_tiles = \
            {'SVA1-ABELL-1361':['DES0424-5957', 'DES0430-5957', 'DES0435-5957', 'DES0426-6039', 
                                'DES0431-6039'],

             'SVA1-SPT-CLJ0040-4407':['DES0035-4457', 'DES0038-4331', 'DES0038-4414', 'DES0039-4457', 
                                      'DES0042-4331', 'DES0042-4414', 'DES0043-4457', 'DES0045-4331'],

             'SVA1-SPT-CLJ0438-5419':['DES0509-5414', 'DES0511-5457', 'DES0513-5331', 'DES0514-5414', 
                                      'DES0516-5457', 'DES0518-5331', 'DES0519-5414', 'DES0521-5457', 'DES0522-5331'],

             'SVA1-SPT-CLJ0509-6118':['DES0503-6122', 'DES0506-6039', 'DES0506-6205', 'DES0509-6122',
                                      'DES0512-6039', 'DES0512-6205', 'DES0515-6122'] }

    sva1_clusters=[]
    for release in release_tiles:
        for r in release_tiles[release]:
            if r not in sva1_clusters:
                sva1_clusters.append(r)

    release_tiles['SVA1-CLUSTERS1'] = sva1_clusters

    return release_tiles

def get_adhoc_release_dir():
    desdata=get_des_rootdir()
    d=os.path.join(desdata,'syncfiles','adhoc-releases')
    return d

def get_adhoc_release_file():
    d=get_adhoc_release_dir()
    mapfile=os.path.join(d, 'sva1-adhoc-releases.json')

    return mapfile

def gen_release_runs():
    import json
    d=get_adhoc_release_dir()
    sva1_runs = get_release_runs('sva1_coadd')

    release_map = {}
    release_tiles = _get_adhoc_release_tiles()
    for r in release_tiles:
        tiles = release_tiles[r]

        fname=os.path.join(d,'coadd-runlist-%s.txt' % r)

        release_map[r] = fname

        print fname
        with open(fname,'w') as fobj:
            for tile in tiles:
                for run in sva1_runs:
                    if tile in run:
                        fobj.write('%s\n' % run)

    mapfile=get_adhoc_release_file()
    print mapfile
    with open(mapfile,'w') as fobj:
        json.dump( release_map, fobj, indent=1, separators=(',', ':'))


# 2,61 not processed.
# 31 is processed in hope we can arrive at a fix, but won't use
# in science or exposure checker for now.
SKIP_CCDS=[2,31,61]
# this is a csv, e.g. 32,55,62
SKIP_CCDS_CSV=','.join([str(ccd) for ccd in SKIP_CCDS])

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


def get_coadd_info_by_release(release, band, withbands=None):
    """
    withbands means only select tiles for which we have all the
    bands
    """
    runlist = get_release_runs(release, withbands=withbands)

    data = get_coadd_info_by_runlist(runlist, band)
    return data

def get_coadd_srclist_by_release(release, withbands, **keys):
    """
    Get all the coadd runs associated with the release, and then
    grab the source information for the SE images that made up
    the coadd

    See the _load_srclist method in the Coadd object for what
    fields will be present
    """

    print >>stderr,'getting coadd_runs with bands:',withbands
    coadd_runs=get_release_runs(release,
                                withbands=withbands,
                                **keys)

    ncoadd=len(coadd_runs)
    print >>stderr,'extracting source lists'

    # use dict so we only get unique ones
    fdict={}
    for i,coadd_run in enumerate(coadd_runs):
        print >>stderr,'%d/%d' % (i+1,ncoadd),coadd_run,
        for band in withbands:
            print >>stderr,band,
            cf=Coadd(coadd_run=coadd_run, band=band, **keys)

            cf.load(srclist=True)
            if i==0 and band==withbands[0]:
                print >>stderr,'\n'
                pprint(cf.srclist[0],stream=stderr)
                print >>stderr,'\n'

            for fd in cf.srclist:
                key='%s-%s' % (fd['expname'], fd['ccd'])
                fdict[key] = fd

        print >>stderr,""

    print >>stderr,'converting to list of dicts'
    data = [fdict[key] for key in fdict]
    return data

_runexp_template="""
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
    and image.ccd not in (%(skip_ccds)s)\n"""

_run_template="""
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
    and image.ccd not in (%(skip_ccds)s)\n"""

def get_red_info_by_run(run, expname=None, skip_ccds=SKIP_CCDS, conn=None, **keys):
    """
    Get some red info for the input run and possibly exposurename

    parameters
    ----------
    run: string
        run identifier
    expname: optional
        exposurename
    """

    if conn is None:
        conn=desdb.Connection(**keys)

    skip_ccds=','.join(skip_ccds)
    desdata=get_des_rootdir()
    if expname is not None:
        query=_runexp_template % {'run':run,
                                  'expname':expname,
                                  'desdata':desdata,
                                  'skip_ccds':skip_ccds}
    else:
        query=_run_template % {'run':run,
                               'desdata':desdata,
                               'skip_ccds':skip_ccds}

    data=conn.quick(query)

    return data

def get_red_info_by_runlist(runlist,
                            explist=None,
                            **keys):
    """
    runlist and explist are paired
    """

    conn=desdb.Connection(**keys)

    desdata=get_des_rootdir()
    dlist=[]

    if explist is not None:
        for run,expname in zip(runlist,explist):

            data = get_red_info_by_run(run, expname=expname)
            data=conn.quick(query)

            dlist += data
    else:
        nrun=len(runlist)
        for i,run in enumerate(runlist):

            print >>stderr,"    %d/%d %s" % (i+1,nrun,run)

            data = get_red_info_by_run(run)
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
        and image.ccd not in (%(SKIP_CCDS)s)\n"""

    desdata=get_des_rootdir()
    query=query % {'runcsv':runcsv,
                   'desdata':desdata,
                   'SKIP_CCDS':SKIP_CCDS_CSV}

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


def get_red_info_by_release(release,
                            bands=None, 
                            user=None,
                            password=None,
                            host=None,
                            show=True):

    print >>stderr,"getting runlist"
    runs=get_release_runs(release)
    print >>stderr,"getting info by runlist"
    dlist = get_red_info_by_runlist(runs)
    
    if bands is not None:
        print >>stderr,"selecting bands:",bands
        if isinstance(bands,basestring):
            bands=[bands]

        dlist_new=[d for d in dlist if d['band'] in bands]
        dlist=dlist_new
    return dlist


def get_red_info_release_byexp(release,
                               bands=None, 
                               user=None,
                               password=None,
                               show=True,
                               doprint=False, fmt='json'):

    infolist = get_red_info_by_release(release, bands=bands, 
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
                pprint(el)
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
            vals=(len(res),self.coadd_run,self.band)
            raise ValueError("got %d entries for coadd_run=%s band=%s" % vals)
        if len(res)==0:
            vals=(self.coadd_run,self.band)
            print query
            raise ValueError("got no entries for coadd_run=%s band=%s" % vals)

        if len(res) > 0:
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

        Note for non-psf homogenized coadds, will are using one less level.
        See Bob's email.
        """

        desdata=get_des_rootdir()
        query="""
        SELECT
            magzp,
            coadd.band as band,
            d.id,
            loc.run,
            loc.exposurename as expname,
            loc.ccd
        FROM
            coadd_src,coadd,image c,image d, location loc
        WHERE
            coadd.band='{band}'
            and coadd_src.coadd_imageid=coadd.id
            and coadd.run='{coadd_run}'
            and c.id=coadd_src.src_imageid
            and c.parentid=d.id
            and loc.id = d.id\n"""


        query=query.format(band=self['band'],
                           coadd_run=self['coadd_run'])

        res = self.conn.quick(query, show=self.verbose)

        

        df=DESFiles(fs=self.fs)
        srclist=[]
        for r in res:
            for type in ['image','bkg','seg','cat']:
                ftype='red_%s' % type
                url=df.url(ftype,
                           run=r['run'],
                           expname=r['expname'],
                           ccd=r['ccd'])
                r[ftype] = url

            r['astro_refine'] = df.url('astro_refine',
                                       coadd_run=self['coadd_run'],
                                       expname=r['expname'],
                                       ccd=r['ccd'])


            srclist.append(r)

        self.srclist=srclist

        return

    def _load_srclist_old(self):
        query_psf_hmg="""
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

        print 'found',len(idlist),'ids'
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



class DESFiles(dict):
    """
    Generate file urls/paths from filetype, run, etc.

    The returned name is a local path or web url.  The generic name "url" is
    used for both.

    parameters
    ----------
    fs: string, optional
        The file system.  Default is DES_DEFAULT_FS

    version: string, optional
        Optional string, default 'v1' for SVA and Y1 releases.
    """
    def __init__(self, **keys):
        self._setup(**keys)

    def _setup(self, **keys):
        self['version']=keys.get('version','v1')

        self['fs'] = keys.get('fs',get_default_fs())

        root=get_des_rootdir(fs=self['fs'])
        if self['version']=='v2beta':
            root=os.path.join(root, 'Prodbeta','archive')
        self['root']=root

    def get_root(self):
        """
        get the root directory
        """
        return self['root']
    root=get_root

    def get_dir(self, type=None, **keys):
        """
        Get the DES directory for the input file type

        parameters
        ----------
        type: string
            The directory type, e.g. 'red_immask', 'red_bkg' etc.  See the _fs
            dict.  If None, the root directory is returned

        **keys: Keywords
            Keywords specifying parts of the name.  See
            the docs for expand_desvars
        """

        if type is None:
            return self.root()

        fdict=self._get_fdict()

        if type not in fdict:
            raise ValueError("Unsupported path type '%s'" % type)
        
        if self['fs'] == 'net':
            url = fdict[type]['remote_dir']
        else:
            url = fdict[type]['dir']

        url = self._expand_desvars(url, **keys)
        return url

    dir=get_dir

    def get_url(self, type=None, **keys):
        """
        Get the URL (local or remote) for the file type.

        possible parameters (there could be others required)
        -------------------
        type: string
            The file type, e.g. 'red_immask', 'red_bkg'.
            dict.  If None, the root directory is returned
        run: string
            The run id

        **keys: Keywords
            Keywords specifying parts of the name.  See
            the docs for expand_desvars
        """
        if type is None:
            return self.get_root()

        fdict=self._get_fdict()

        url = self.dir(type, **keys)

        if 'name' in fdict[type]:
            url = os.path.join(url, fdict[type]['name'])

        url = self._expand_desvars(url, **keys)
        return url

    name=get_url
    url=get_url

    def _expand_desvars(self, url, **keys):
        keys['fs'] = self['fs']
        keys['version'] = self['version']
        return expand_desvars(url, **keys)

    def _get_fdict(self):
        if self['version']=='1':
            fdict=fnames_v1
        elif self['version']=='v2beta':
            fdict=fnames_v2beta
        else:
            raise ValueError("bad version: '%s'" % self['version'])
        return fdict

def get_dir_generic(subdirs):
    if not isinstance(subdirs, list):
        raise ValueError("subdirs must be a list")

    root=get_des_rootdir()
    dirs=[root] + subdirs

    dir=os.path.join(*dirs)
    return dir
 
def get_path_generic(subdirs, fileparts, join_char='_', ext='fits'):

    dir=get_dir_generic(subdirs)

    if not isinstance(fileparts, list):
        raise ValueError("fileparts must be a list")

    fname=join_char.join(fileparts)
    fname = '%s.%s' % (fname,ext)

    return os.path.join(dir, fname)


# notes 
#   - .fz might not always hold
#   - EXPNAME can also be built from POINTING-BAND-VISIT
fnames_v1={}
fnames_v1['red_run']   = {'remote_dir':'$DESREMOTE/$DESPROJ/red/$RUN/red',
                    'dir':         '$DESDATA/$DESPROJ/red/$RUN/red'}

fnames_v1['red_exp']   = {'remote_dir':'$DESREMOTE/$DESPROJ/red/$RUN/red/$EXPNAME',
                    'dir':       '$DESDATA/$DESPROJ/red/$RUN/red/$EXPNAME'}
fnames_v1['red_qa']   = {'remote_dir':'$DESREMOTE/$DESPROJ/red/$RUN/QA/$EXPNAME',
                   'dir':       '$DESDATA/$DESPROJ/red/$RUN/QA/$EXPNAME'}



fnames_v1['red_image'] = {'remote_dir':fnames_v1['red_exp']['remote_dir'],
                    'dir':       fnames_v1['red_exp']['dir'], 
                    'name':'$EXPNAME_$CCD.fits.fz'}

fnames_v1['red_cat']   = {'remote_dir':fnames_v1['red_exp']['remote_dir'],
                    'dir':       fnames_v1['red_exp']['dir'], 
                    'name':'$EXPNAME_$CCD_cat.fits'}
fnames_v1['red_bkg']   = {'remote_dir':fnames_v1['red_exp']['remote_dir'],
                    'dir':       fnames_v1['red_exp']['dir'], 
                    'name':'$EXPNAME_$CCD_bkg.fits.fz'}
fnames_v1['red_seg']   = {'remote_dir':fnames_v1['red_qa']['remote_dir'],
                    'dir':       fnames_v1['red_qa']['dir'], 
                    'name':'$EXPNAME_$CCD_seg.fits.fz'}


fnames_v1['se_generic'] = {'dir': '$DESDATA/$DESPROJ/$FILECLASS/$RUN/$FILETYPE',
                     'name':'$EXPNAME_$CCD_header.fits'
                                  }


fnames_v1['coadd_run'] = {'remote_dir': '$DESREMOTE/$DESPROJ/coadd/$COADD_RUN/coadd',
                    'dir':        '$DESDATA/$DESPROJ/coadd/$COADD_RUN/coadd'}
fnames_v1['coadd_qa'] = {'remote_dir': '$DESREMOTE/$DESPROJ/coadd/$COADD_RUN/QA/segmap',
                   'dir':        '$DESDATA/$DESPROJ/coadd/$COADD_RUN/QA/segmap'}

fnames_v1['coadd_image'] = {'remote_dir': fnames_v1['coadd_run']['remote_dir'],
                      'dir':        fnames_v1['coadd_run']['dir'], 
                      'name':       '$TILENAME_$BAND.fits.fz'}
fnames_v1['coadd_cat']   = {'remote_dir': fnames_v1['coadd_run']['remote_dir'],
                      'dir':fnames_v1['coadd_run']['dir'], 
                      'name':'$TILENAME_$BAND_cat.fits'}
fnames_v1['coadd_seg']   = {'remote_dir': fnames_v1['coadd_qa']['remote_dir'],
                      'dir':fnames_v1['coadd_qa']['dir'], 
                      'name':'$TILENAME_$BAND_seg.fits.fz'}

fnames_v1['astro_refine'] = {'remote_dir':'$DESREMOTE/$DESPROJ/coadd/$COADD_RUN/QA/coadd_astrorefine_head',
                       'dir':'$DESDATA/$DESPROJ/coadd/$COADD_RUN/QA/coadd_astrorefine_head',
                       'name':'$EXPNAME_$CCD.head'}

# fits versions of the .head files
fnames_v1['astro_refine_fits'] = {'dir':'$DESDATA/EXTRA/coadd/$COADD_RUN/QA/coadd_astrorefine_head',
                            'name':'$EXPNAME_$CCD_head.fits'}


# for refactored pipeline, beta
# note hard-coded path elements such as Y2T and Y2A1, don't know yet where
# to get these
fnames_v2beta={}
fnames_v2beta['red_immask'] = \
    {'remote_dir':'$DESREMOTE/Prodbeta/archive/$PROJECT/finalcut/Y2T/Y2A1-r$REQNUM/D$EXPNUM/p$ATTNUM/red/immask',
     'dir':'$DESDATA/Prodbeta/archive/$PROJECT/finalcut/Y2T/Y2A1-r$REQNUM/D$EXPNUM/p$ATTNUM/red/immask',
     'name':'D$EXPNUM_$BAND_c$CCDNUM_r$REQNUMp$ATTNUM_immasked.fits'}
fnames_v2beta['red_bkg'] = \
    {'remote_dir':'$DESREMOTE/Prodbeta/archive/$PROJECT/finalcut/Y2T/Y2A1-r$REQNUM/D$EXPNUM/p$ATTNUM/bkg',
     'dir':'$DESDATA/Prodbeta/archive/$PROJECT/finalcut/Y2T/Y2A1-r$REQNUM/D$EXPNUM/p$ATTNUM/bkg',
     'name':'D$EXPNUM_$BAND_c$CCDNUM_r$REQNUMp$ATTNUM_bkg.fits'}


def expand_desvars(string_in, **keys):
    """
    expand DES specific variables, as well as environment variables
    """

    version=keys.get('version','v1')

    if version=='v1':
        return expand_desvars_v1(string_in, **keys)
    else:
        return expand_desvars_v2(string_in, **keys)

def expand_desvars_v2(string_in, **keys):
    string=string_in
    root=get_des_rootdir(**keys)
    root_remote=get_des_rootdir(fs='net')


    if '$DESDATA' in string:
        string = string.replace('$DESDATA', root)

    if '$TMPDIR' in string:
        tmpdir=os.environ['TMPDIR']
        string = string.replace('$TMPDIR', tmpdir)

    if '$DESREMOTE' in string:
        string = string.replace('$DESREMOTE', root_remote)

    if '$PROJECT' in string:
        project=keys.get('project', None)
        if project is None:
            project=get_default_des_project()

        string = string.replace('$PROJECT', str(project))

    if '$REQNUM' in string:
        reqnum = keys.get('reqnum',None)
        if reqnum is None:
            raise ValueError("reqnum must be sent: '%s'" % string_in)

        string = string.replace('$REQNUM', str(reqnum))

    if '$EXPNUM' in string:
        expnum = keys.get('expnum',None)
        if expnum is None:
            raise ValueError("expnum must be sent: '%s'" % string_in)

        string = string.replace('$EXPNUM', '%08d' % expnum)

    if '$ATTNUM' in string:
        attnum = keys.get('attnum',None)
        if attnum is None:
            raise ValueError("attnum must be sent: '%s'" % string_in)

        string = string.replace('$ATTNUM', '%02d' % attnum)

    if '$CCDNUM' in string:
        ccdnum = keys.get('ccdnum',None)
        if ccdnum is None:
            ccdnum = keys.get('ccd',None)
            if ccdnum is None:
                raise ValueError("ccd= or ccdnum= must be sent: '%s'" % string_in)

        string = string.replace('$CCDNUM', '%02d' % ccdnum)


    if '$BAND' in string:
        band = keys.get('band',None)
        if band is None:
            raise ValueError("band must be sent: '%s'" % string_in)

        string = string.replace('$BAND', band)


    # finally do all environment variables
    string = os.path.expandvars(string)

    # see if there are any leftover un-expanded variables.  If so
    # raise an exception
    if '$' in string:
        raise ValueError("There were unexpanded variables: '%s'" % string)

    return string
   


def expand_desvars_v1(string_in, **keys):
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

    if string.find('$FILECLASS') != -1:
        fileclass=keys.get('fileclass', None)
        if fileclass is None:
            raise ValueError("fileclass keyword must be sent: '%s'" % string_in)
        string = string.replace('$FILECLASS', str(fileclass))


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

    # finally do all environment variables
    string = os.path.expandvars(string)

    # see if there are any leftover un-expanded variables.  If so
    # raise an exception
    if string.find('$') != -1:
        raise ValueError("There were unexpanded variables: '%s'" % string)

    return string

