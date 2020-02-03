#!/usr/bin/python

import csv
import json
from collections import OrderedDict
import argparse
import json
import traceback
import os
import tables
import tool

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# name, description, dataSourceId, configurationScopeName, metadataUrl, oauth2RenderScopeName, oauth2ReportScopeName, oauth2ViewScopeName, featureViewCategoryId
tool.add_argument( '--features', '-f', 
    help    = 'CSV containing features',
    dest    = 'features',
    default = tables.mapLayers )

tool.add_argument( '--base-dir', '-b', 
    help    = 'name of directory to work in',
    dest    = 'base',
    default = '.' )

tool.add_argument( '--constants', '-c', 
    help    = 'JSON containing constant columns',
    dest    = 'constants',
    default = 'constants.json' )

tool.add_argument( '--out-dir', '-d', 
    help    = 'name of directory to receive files',
    dest    = 'dir',
    default = 'geoserver/layers' )

tool.add_argument( '--manifest', '-m', 
    help    = 'name of file to receive manifest of files created',
    dest    = 'manifest',
    default = 'geoserver-layers.manifest' )

arg = tool.args();

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

constant = {}
fn = arg.base + '/' + arg.constants
try:
    with open( fn, 'rb' ) as c:
        constant = json.load( c )
except Exception as e:
    fn = arg.constants
    with open( fn, 'rb' ) as c:
        constant = json.load( c )
print 'constants loaded from {0}'.format( fn )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 
feature = OrderedDict()
const = constant.get( 'feature', {} )
for f in tables.fetchRows( arg.features ):
# with open( arg.features, 'rb' ) as features:
    # for f in csv.DictReader( features, skipinitialspace=True ):
        f = dict( f )

        if f['name'] in feature:
            print 'feature '+f['name']+' duplicated, skipping'
            continue

        for k in const:
            if not k in f or not f[k]:
                f[ k ] = const[ k ]

        # print f
        feature[ f['name'] ] = f

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def createLayer( f ):
    return OrderedDict( [
        ( "workspace", f['workspace'] ),
        ( "datastore", f['dataStore'] ),
        ( "name",      f['name']      ), 
        ( "title",     f.get( 'title', f['name'] )     ),
        ( "styleName", f.get( 'styleName', f['name'] ) )
    ] )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def json_out( data ):
    return json.dumps( data, sort_keys=False, indent=2, separators=(',', ': ') )

if not os.path.isdir( arg.base + '/' + arg.dir ):
    os.makedirs( arg.base + '/' + arg.dir )

with open( arg.base + '/' + arg.manifest, 'w' ) as mout:
    for fname in feature:
        ifn = arg.dir + '/' + fname + '.json'
        fn = arg.base + '/' + ifn
        with open( fn, 'w' ) as out:
            out.write( json_out( [ createLayer( feature[ fname ] ) ] ) )
            tool.INFO( '{} written', fn )
            mout.write( ifn + '\n' )

tool.INFO( '{} written', arg.manifest )



