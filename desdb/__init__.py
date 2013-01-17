# files location code is useful even without oracle
from . import files
from . import sync

from .files import DESFiles

# catch error if oracle is not found
try:
    from . import desdb

    from desdb import connect
    from desdb import Connection
    from desdb import CursorWriter 
    from desdb import ObjWriter
    from desdb import print_cursor
    from desdb import cursor2dictlist
except:
    pass


