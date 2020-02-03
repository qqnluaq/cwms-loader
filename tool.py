import json 
import sys
import argparse
from prompt import prompt 
import traceback
from contextlib import contextmanager

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

_parser = argparse.ArgumentParser( formatter_class=argparse.ArgumentDefaultsHelpFormatter )

_parser.add_argument( 'filename', 
    help    = 'filename to process. If filename starts with @, it contains a list files to import',
    nargs   = '?', 
    metavar = 'JSON_FILE' )

_parser.add_argument( '--dryrun', 
    help    = 'dont change anything',
    action  = 'store_true' )

_parser.add_argument( '--verbose', '-v', 
    help    = 'be chatty', 
    action  = 'store_true' )

_parser.add_argument( '--env',  
    help    = 'environment config python file',
    nargs   = 1 )

_parser.add_argument( '--stop',  
    help    = 'stop on first error', 
    action  = 'store_true' )

def add_argument( *args, **kw ):
    _parser.add_argument( *args, **kw )

_args = [0]
def args():
    if not _args[0]:
        _args[0] = _parser.parse_args()
    return _args[0]

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

_indent = [0]

def STDERR( m='', *params ):
    nl = '\n'
    if m and m[-1] == '\\':
        m = m[:-1]
        nl = ''

    indent = ' ' * (2 * _indent[0])

    if params:
        sys.stderr.write( indent + m.format( *params ) + nl )
    else:
        sys.stderr.write( indent + m + nl )

@contextmanager
def INDENT( delta=1 ):
    _indent[0] += delta
    yield
    _indent[0] -= delta

def INFO( m='', *params ):
    STDERR( m, *params )

status_width = [0]
def STATUS( m='', *params ):
    if args().verbose:
        return

    s = m
    if params:
        s = m.format( *params )

    l = len( s.decode('utf-8') )
    sys.stderr.write( '\010' * status_width[0] )
    sys.stderr.write( s )
    sys.stderr.write( ' ' * max(0, status_width[0] - l) )
    sys.stderr.write( '\010' * max(0, status_width[0] - l) )

    status_width[0] = l

def VERBOSE( m='', *params ):
    if args().verbose:
        STDERR( m, *params )


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

@prompt( 'Enter name of environment (int,test,prod): ' )
def environment_name():
    if args().env == None or len(args().env) == 0:
        return

    return args().env[0]

def import_environment( name=None ):
    if name == None:
        name = environment_name()

    VERBOSE( 'Loading environment ' + name )
    try:
        return getattr( __import__( 'env.' + name ), name )
    except Exception as e:
        VERBOSE( str( e ) )
        return

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

_filenames = []
def get_filenames():
    if _filenames:
        return _filenames

    filename = args().filename
    seen = {}
    if filename[0] == '@':
        try:
            with open( filename[1:], 'rb') as file:
                for line in file:
                    name = line.rstrip('\r\n')
                    if not name in seen:
                        seen[ name ] = True
                    else:
                        VERBOSE( '{0} is appears more than once, skipping'.format( name ) )

                    _filenames.append( name )

        except Exception as e:
            VERBOSE( traceback.format_exc() )
            INFO( "failed to read {0}", filename[1:] )
            return
    else:
        _filenames.append( filename )

    return _filenames

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

if __name__ == "__main__":
    args()
