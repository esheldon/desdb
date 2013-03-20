import os
from urlparse import urlparse
import urllib2
import netrc
import ConfigParser

class PasswordGetter:
    """
    Try to get username/password from different sources.

    The types to try are listed in the types= keyword as a list.
    Defaults to only trying netrc

    Allowed types are
        'netrc','desservices','desdb_pass' (deprecated)

    netrc is much more general, as it can be used for any url.
    desservices is only used for the database connection.
    """
    def __init__(self, url, types=['netrc','desservices']):
        self.url=url
        self.host = urlparse(self.url).hostname

        self.types=types
        
        self.type=None
        self.password=None
        self.user=None
        for type in types:
            if self._set_username_password(type):
                self.type=type
                break

    def _set_username_password(self, type):
        gotit=False
        if type=='netrc':
            gotit=self._try_netrc()
        elif type=='desservices':
            gotit=self._try_desservices(fname)
        elif type=='desdb_pass':
            gotit=self._try_desdb_pass(fname)
        else:
            raise ValueError("expected type 'netrc' or "
                             "'desservices' or 'desdb_pass'")

        return gotit

    def _try_netrc(self):
        res=netrc.netrc().authenticators(self.host)

        if res is None:
            # no authentication is needed for this host
            return False

        (user,account,passwd) = res
        self.user=user
        self.password=passwd

        return True

    def _try_desservices(self):
        fname=os.path.expanduser('~/.desservices')
        if not os.path.exists(fname):
            return False

        config = ConfigParser.ConfigParser()
        config.read(fname)
        self.user=config.get('desdb', 'user')
        self.password=config.get('desdb', 'passwd')

    def _try_desdb_pass(self):
        fname=os.path.expanduser('~/.desdb_pass')
