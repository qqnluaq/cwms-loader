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

_parser.add_argument( '--verbose', '-v', 
    help    = 'be chatty', 
    action  = 'store_true' )

_parser.add_argument( '--env',  
    help    = 'environment config python file',
    nargs   = 1 )

_parser.add_argument( '--base-dir', '-b', 
    help    = 'name of directory to work in',
    dest    = 'base',
    default = 'DUMP/query-templates' )

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

# out = sys.stdout
# if not args.filename == '-':
#     out = open( args.filename, 'w' )

if not os.path.isdir( args.base ):
    tool.INFO( 'create {0}', args.base )
    os.makedirs( args.base )

svc = service[ 'sos' ]


# ok0, ver0, data0 = svc.get_element( '/dataSources' )

# if not ok0:
#     tool.INFO( 'ds failed' )
#     exit(1)

# dataSourceName={}
# for ds in data0['dataSources']:
#     dataSourceName[ ds['id'] ] = ds['name']

qts = [
    "All",
    "FishWildlife",
    "Forest",
    "Land",
    "Mining",
    "OilGas",
    "Water"
]

qtlout = {}
for qt in qts:
    kw = {}
    kw['params'] = { 'pageRowCount': 2000 }
    ok, ver, data = svc.get_element( '/queryTemplates/{0}/queryTemplateLayers'.format( qt ), **kw )

    if not ok:
        tool.INFO( 'failed' )
        exit(1)

    for qtl in sorted( data['queryTemplateLayers'], key=lambda k:k['name'] ):    
        name = qtl['name']
        if not name.endswith('_SVW'):
            continue

        qtlout[ '{0}-{1}'.format( qt, name ) ] = [ qtl ]

        tool.INFO( 'QTL: {0}.{1}', qt, name )
        del qtl['links']
        # fc['dataSourceId'] = dataSourceName[ fc['dataSourceId'] ]
        del qtl['id']

        qtl['associatedFeatureViewId'] = name
        del qtl['queryTemplateId']
        # qtl['queryTemplateId'] = qt

        del qtl["isTransactionTimeEnabled"]
        del qtl["isValidTimeEnabled"]
        del qtl["templateLayerAttributes"]

        qtl['@type']='queryTemplateLayer'
        qtl['@mode']='recreate'
        qtl['@parents']=[{'name': qt}]

        qtla = []
        qtl['@children']=qtla

        # qtl['@preDelete'] = [ "featureViews/" + name ]
        #   "featureViews/" + name
        # ],
        
        ok1, ver1, data1 = svc.get_element( '/queryTemplateLayers/{0}.{1}/templateLayerAttributes'.format(qt,name) )
        if ok1:       
            for sa in sorted( data1['templateLayerAttributes'], key=lambda k:int(k['attributeOrder']) ):
                tool.INFO( '  QTLA: {1}={0}', sa['name'], sa['attributeOrder'] )
                del sa['links']
                del sa['id']
                sa['associatedFeatureViewAttributeId']='{0}.{1}'.format(name,sa['name'])
                # sa['queryTemplateLayerId']='{0}.{1}'.format(qt,name)
                del sa['queryTemplateLayerId']
                sa['@type']='templateLayerAttribute'
                sa["@mode"]= "recreate"
                
                qtla.append( sa )
        
        # if len(qtlout) >= 3:
            # break

sfn = 'diff-query-templates.sh'
with  open( sfn, 'w' ) as sout:
    for fcname in sorted( qtlout ):
        fn = args.base + '/' + fcname + '.json'
        with  open( fn, 'w' ) as out:
            tool.INFO( 'write: {0}', fn )
            out.write( json.dumps( qtlout[fcname], sort_keys=True, indent=2, separators=(',', ': ') ) )
        
        sout.write( 'echo\necho {0}\n'.format( fcname ) )
        sout.write( 'json-diff -C {0} DEV/query-templates/{1}.json\n'.format( fn, fcname ) )

