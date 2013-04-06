import sys
from sys import stderr
import os
import tempfile

import urllib2
from urlparse import urlparse
import shutil

class URLLister(object):
    """
    Get a list of all urls under the specified remote directory.

    Iterate over the result, e.g.

        lister=URLLister(url)
        for url in lister:
            print url

    Or get a list of urls
        lister.get_urls()
    """
    def __init__(self, url, use_netrc=False):
        self.url=url
        self.use_netrc=use_netrc

        self.opener=self._get_opener()

        self._get_url_list()

    def get_urls(self):
        return self.url_list

    def _get_url_list(self):

        f = self.opener.open(self.url)

        url_list=[]
        for line in f:
            if 'Parent Directory' in line:
                continue
            elif '?' in line:
                continue
            elif 'href' in line:
                ls=(line.split('href="'))[1]
                fname=(ls.split('"'))[0]
                
                url=os.path.join(self.url, fname)
                url_list.append(url)

        self._url_list=url_list

    def _get_opener(self):
        """
        Note the this ProxyBasicAuthHandler proxy handler looks for $http_proxy
        or $ftp_proxy
        """

        authinfo=None
        if self.use_netrc:
            authinfo=self._get_netrc_auth()

        if authinfo is not None:
            opener = urllib2.build_opener(urllib2.ProxyBasicAuthHandler,
                                          authinfo)
            if opener is None:
                mess="Could not create openerfor url %s; check netrc" % url
                raise RuntimeError(mess)
        else:
            opener = urllib2.build_opener(urllib2.ProxyBasicAuthHandler)

        return opener

    def _get_netrc_auth(self):
        import netrc


        host = urlparse(self.url).hostname
        res=netrc.netrc().authenticators(host)

        if res is None:
            # no authentication is needed for this host
            return None

        (user,account,passwd) = res

        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, 
                                  self.url, 
                                  user, 
                                  passwd)

        authinfo = urllib2.HTTPBasicAuthHandler(password_mgr)

        return authinfo

    def __iter__(self):
        self._current=0
        return self

    def next(self):
        if self._current < len(self._url_list):
            url=self._url_list[self._current]
            self._current += 1
            return url
        else:
            raise StopIteration

class Synchronizer(object):
    """
    sync files from the remote directory to the local directory

    Files are listed in the remote directory and copied.  Time stamps are
    checked and only newer files are copied unless clobber=True

    parameters
    ----------
    remote_url: string
        The remote directory
    local_dir: string
        The local directory

    use_netrc: bool, optional
        This is for listing the remote directory; authentication
        can be gotten from your ~/.netrc file.
        
        For copies, netrc is *always* used if needed by sending the
        --netrc-optional option to curl.

        default False
    clobber: bool, optional
        over-write existing files
        default False
    show_progress: bool, optional
        If True, curl will show progress as each file is copied.
        default False
    ntry: int, optional
        Number of retries if a copy fails.  Default 10
    debug: bool, optional
        if True, show every step of the procedure

    example
    -------

    syncer=Synchronizer(remote_url, local_url)
    syncer.sync()

    method
    ------

    Time stamping is straightforward: we use the -z flag for curl.  Could do it
    all in python but the timezone issues are too much to deal with.

    Always copy to a temporary file in the output directory and then move the
    file.  This way we don't end up with a half-copied file in the final
    location.

    """
    def __init__(self, remote_url, local_dir, 
                 use_netrc=False,
                 clobber=False,
                 show_progress=False, 
                 ntry=10,
                 debug=False):

        self.local_dir=local_dir
        self.clobber=clobber
        self.debug=debug
        self.show_progress=show_progress
        self.ntry=ntry

        self.url_lister=URLLister(remote_url,
                                  use_netrc=use_netrc)

    def sync(self):
        for url in self.url_lister:
            self.sync_file(url)

    def sync_file(self, url):
        local_path, tmp_path=self._get_local_paths(url)
        try:

            local_exists=os.path.exists(local_path)
            cmd=self._get_curl_command(url, local_path, tmp_path, local_exists)

            if self.debug:
                print >>stderr,cmd

            self._run_curl(cmd, url)

            # We need to check because if the local file already existed and
            # was no older than the remote, no file was downloaded
            if os.path.exists(tmp_path):
                # note we usually only print the file name if a copy was made
                print url
                self._move_from_tmp(local_path, tmp_path)

        except KeyboardInterrupt:
            sys.exit(1)
        finally:
            # e.g. if the user hit ctrl-c we still want to clean up
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def _run_curl(self, cmd, url):
        itry=0
        while itry < self.ntry:
            res=os.system(cmd)
            if res==2:
                # was probably an interrupt
                mess=("Got curl error: 2 for url %s. "
                      "Probably interrupt, stopping" % url)
                print mess
                sys.exit(1)

            if res != 0:
                mess="Got curl error: %s for url %s" % (res,url)
                print mess
            itry += 1

        if res != 0:
            print 'giving up after %d tries for url %s' % (self.ntry,url)
            print 'command was %s' % cmd

    def _move_from_tmp(self, local_path, tmp_path):
        if os.path.exists(local_path):
            if self.debug:
                print 'removing existing file:',local_path
            os.remove(local_path)

        if self.debug:
            print 'moving',tmp_path,'to',local_path
        shutil.move(tmp_path, local_path)

    def _get_local_paths(self, url):

        bname=os.path.basename(url)
        local_path=os.path.join(self.local_dir, bname)

        tmp_path=tempfile.mktemp(prefix=bname+'-',dir=self.local_dir)

        return local_path, tmp_path

    def _get_curl_command(self, url, local_path, tmp_path, local_exists):

        # -k: don't bother with certificates
        # --netrc-optional: use ~/.netrc for authentication if needed
        # --create-dirs: make dirs necessary to create the output file
        # --remote-time: give the local file the remote file timestamp
        #
        # note the tmp_path is under the same directory as local_path,
        # so --create-dirs still does what we need

        cmd=['curl -k --netrc-optional --create-dirs --remote-time']

        if not self.show_progress:
            cmd += ['-s']

        if local_exists and not self.clobber:
            cmd += ['-z "{local_path}"']

        cmd += ['-o "{tmp_path}" "{url}"']
        cmd = ' '.join(cmd)
        cmd=cmd.format(local_path=local_path,
                       url=url,
                       tmp_path=tmp_path)
        return cmd

