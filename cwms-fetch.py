#!/usr/bin/python

import requests
import json
import sys
import argparse
import os
import os.path
import urllib
import traceback
import cwm
import inspect 
from collections import OrderedDict
import tool 
import re

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

_parser = argparse.ArgumentParser( formatter_class=argparse.ArgumentDefaultsHelpFormatter )

_parser.add_argument( 'filename', 
    help    = 'output filename. If filename starts with @, it contains a list files to import',
    nargs   = '?',
    default = '-',
    metavar = 'OUTPUT_FILE' )

_parser.add_argument( '--service', '-s',
    help    = 'service',
    default = 'cwms',
    nargs   = 1 )

_parser.add_argument( '--path', '-p',
    help    = 'object to fetch',
    default = '',
    nargs   = '?' )

_parser.add_argument( '--page-rows', '-r',
    help    = 'number of rows to fetch',
    dest    = 'pageRowCount',
    nargs   = '?' )

_parser.add_argument( '--name-filter', '-n',
    help    = 'include names matching pattern',
    default = '',
    nargs   = '?',
    dest    = 'filter' )

_parser.add_argument( '--links', '-l',
    help    = 'include links in output',
    default = False,
    action  = 'store_true' )

_parser.add_argument( '--verbose', '-v', 
    help    = 'be chatty', 
    action  = 'store_true' )

_parser.add_argument( '--env',  
    help    = 'environment config python file',
    nargs   = 1 )

args = _parser.parse_args()
tool._args[0] = args

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

env = tool.import_environment()
if not env:
    tool.tool.INFO( "Could not load environment {0}", environment_name() )
    exit(1)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class Requester:
    def __init__( self, user, password, scope, baseURL ):
        self.user = user
        self.password = password
        self.scope = scope
        self.baseURL = baseURL
        self.headers = None

    def get_headers( self, **extra ):
        if not self.headers:
            query = urllib.urlencode( env.oauthParams( self.scope ) )
            requestUrl = env.oauthBase() + 'oauth/token?' + query 
            tool.VERBOSE( 'GET ' + requestUrl )
            r = requests.get( requestUrl, auth=( self.user, self.password ) )
            if not r.ok:
                tool.VERBOSE( 'response: ' + str(r.status_code) )
                tool.VERBOSE( r.text )
                tool.INFO( 'Failed to get token for {0}', self.user )
                exit(1)

            self.headers = {
                'Authorization': 'Bearer ' + json.loads( r.text )[ 'access_token' ],
                'Content-Type': 'application/json'
            }

        if extra:
            h = self.headers.copy()
            h.update( extra )
            return h

        return self.headers

    def _make_request( self, method, url, **kwargs ):
        tool.VERBOSE( method.upper() + ' ' + url )
        # tool.VERBOSE( json.dumps( kwargs['headers'], sort_keys=True, indent=2, separators=(',', ': ') ) ) 
        if 'data' in kwargs:
            tool.VERBOSE( json.dumps( json.loads(kwargs['data']), sort_keys=True, indent=2, separators=(',', ': ') ) ) 

        # if args.dryrun:
        #     return ( 'dryrun', '', {} )

        r = getattr( requests, method.lower() )( url, **kwargs )

        res = True if r.ok else ( None if r.status_code >= 500 else False )
        ver = eval( r.headers.get( 'ETag', '""') )

        tool.VERBOSE( 'status {0}: {1}', r.status_code, res )
        try:
            blob = json.loads( r.text )

            tool.VERBOSE( json.dumps( blob, sort_keys=True, indent=2, separators=(',', ': ') ) )
            return ( res, ver, blob )

        except:
            tool.VERBOSE( r.text )
            return ( res, ver, r.text )

    def get_element( self, path, **kw ):
        ok, ver, data = self._make_request( 'get', self.baseURL + path, 
            headers = self.get_headers(),
            **kw
        )
        
        try:
            return ( ok, ver, data )
        except:
            return ( ok, ver, None )


service = {
    'cwms': Requester( env.cwmsUser(), env.cwmsPassword(), env.cwmsScope(), env.cwmsBase() ),
    'sos':  Requester( env.sosUser(),  env.sosPassword(),  env.sosScope(),  env.sosBase() )
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

out = sys.stdout
if not args.filename == '-':
    out = open( args.filename, 'w' )

svc = service[ args.service[0] ]

kw = {}
if args.pageRowCount:
    kw['params'] = { 'pageRowCount': args.pageRowCount }

ok, ver, data = svc.get_element( args.path, **kw )

if ok:
    pc = args.path.split( '/' )
    if pc[-1] in data:
        for obj in data[pc[-1]]:
            if not args.links and 'links' in obj:
                del obj['links']

        objs = [ d for d in data[pc[-1]] if re.search( args.filter, d.get('name',d.get('id')) ) ]
        out.write( json.dumps( objs, sort_keys=True, indent=2, separators=(',', ': ') ) )
        tool.INFO( '{0}: {1}', args.path, len(objs) )
    else:
        if not args.links and not data['@type'].endswith('resources') and 'links' in data:
            del data['links']

        out.write( json.dumps( data, sort_keys=True, indent=2, separators=(',', ': ') ) )
        tool.INFO( '{0}: {1}', args.path, 1 )

    out.write( '\n' )
else:
    tool.INFO( '{0} failed', args.path )

