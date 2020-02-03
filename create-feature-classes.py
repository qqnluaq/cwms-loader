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

# parser = argparse.ArgumentParser()

# name, description, dataSourceId, configurationScopeName, metadataUrl, oauth2RenderScopeName, oauth2ReportScopeName, oauth2ViewScopeName, featureViewCategoryId
tool.add_argument( '--features', '-f', 
    help    = 'CSV containing features',
    dest    = 'features',
    default = tables.sosLayers )

# featureName, name, title, order, description, attributeTypeId
tool.add_argument( '--feature-attributes', '-a', 
    help    = 'CSV containing feature attributes',
    dest    = 'feature_attributes',
    default = tables.featureAttributes )

tool.add_argument( '--out-dir', '-d', 
    help    = 'name of directory to receive files',
    dest    = 'dir',
    default = 'feature-classes' )

tool.add_argument( '--manifest', '-m', 
    help    = 'name of file to receive manifest of files created',
    dest    = 'manifest',
    default = 'feature-classes.manifest' )

tool.add_argument( '--constants', '-c', 
    help    = 'JSON containing constant columns',
    dest    = 'constants',
    default = 'constants.json' )

tool.add_argument( '--base-dir', '-b', 
    help    = 'name of directory to work in',
    dest    = 'base',
    default = '.' )

# arg = parser.parse_args()
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
    if f['name'] in feature:
        print 'feature '+f['name']+' duplicated, skipping'
        continue

    for k in const:
        if not k in f or not f[k]:
            f = dict( f )
            f[ k ] = const[ k ]

    feature[ f['name'] ] = f

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

featureAttr = OrderedDict()
const = constant.get( 'featureAttribute', {} )
for fa in tables.fetchRows( arg.feature_attributes ):
    if not fa['featureName'] in feature:
        print 'featureAttribute '+fa['featureName']+' not found in '+arg.features+', skipping'
        continue

    if not fa['featureName'] in featureAttr:
        featureAttr[ fa['featureName'] ] = []

    for k in const:
        if not k in fa or not fa[k]:
            fa = dict( fa )
            fa[ k ] = const[ k ]

    # print fa
    featureAttr[ fa['featureName'] ].append( fa )        

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def createFeatureClass( f, fas ):
    return OrderedDict( [
        ( '@mode',                  'update'                       ),
        ( "@preDelete",             [ "featureViews/" + f['name'] ] ), # add managedWMSServiceLayer
        ( '@type',                  'featureClass'                  ),
        ( "name",                   f['name']                       ),
        ( "description",            f['description']                ),
        ( "dataSourceId",           f['dataSourceId']               ),
        ( "geometryAttrTableName",  f['name']                       ),
        ( "configurationScopeName", f['configurationScopeName']     ),
        ( "@children", [
            OrderedDict( [
                ( '@mode',     'recreate'      ),
                ( "@type",     "sourceTable" ),
                ( "name",      f['name']     ),
                ( "@children", [ createSourceAttribute( fa ) for fa in sorted( fas, key=lambda k:int(k['order']) ) ] )
            ] )
        ] )
    ] )

def createSourceAttribute( fa ):
    return OrderedDict( [
        ( '@mode',           'recreate'            ),
        ( "@type",           "sourceAttribute"     ),
        ( "name",            fa['name']            ),
        ( "attributeOrder",  int(fa['order'])      ),
        ( "description",     fa['description']     ),
        ( "attributeTypeCode", fa['attributeTypeCode'] )
    ] )

def createFeatureView( f, fas ):
    return OrderedDict( [
        ( '@mode',                 'create'              ),
        ( "@type",                 "featureView"              ),
        ( "name",                  f['name']                  ),
        ( "description",           f['description']           ),
        ( "metadataUrl",           f['metadataUrl']           ),
        ( "oauth2RenderScopeName", f['oauth2RenderScopeName'] ),
        ( "oauth2ReportScopeName", f['oauth2ReportScopeName'] ),
        ( "oauth2ViewScopeName",   f['oauth2ViewScopeName']   ),
        ( "featureViewCategoryId", f['featureViewCategoryId'] ),
        ( "featureClassId",        f['name']                  ),
        ( "@children",             [ createFeatureViewAttribute( fa ) for fa in sorted( fas, key=lambda k:int(k['order']) ) ] )
    ] )

def createFeatureViewAttribute( fa ):
    return OrderedDict( [
        ( '@mode',             'create'                                                       ),
        ( "@type",             "featureViewAttribute"                                         ),
        ( "name",              fa['name']                                                     ),
        ( "title",             fa['title']                                                    ),
        ( "attributeOrder",    int(fa['order'])                                               ),
        ( "description",       fa['description']                                              ),
        ( "sourceAttributeId", fa['featureName'] + '.' + fa['featureName'] + '.' + fa['name'] )
    ] )

def createServiceLayer( f ):
    return OrderedDict( [
        ( '@mode',                         'update'                           ),
        ( "@type",                         "managedWMSServiceLayer"           ),
        ( "name",                          f["workspace"] +':'+ f["name"]     ),
        ( "title",                         f["description"]                   ),
        ( "description",                   f["description"]                   ),
        ( "defaultStyleId",                None                               ),
        # ( "defaultStyleId",                f["name"] + '_STYLE'               ),
        ( "associatedFeatureViewId",       f["name"]                          ),
        ( "associatedManagedWMSServiceId", f["associatedManagedWMSServiceId"] ),
        ( "metadataUrl",                   f["metadataUrl"]                   )
    ] )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def json_out( data ):
    return json.dumps( data, sort_keys=False, indent=2, separators=(',', ': ') )

if not os.path.isdir( arg.base + '/' + arg.dir ):
    os.makedirs( arg.base + '/' + arg.dir )

with open( arg.base + '/' + arg.manifest, 'w' ) as man:
    for fname in feature:
        ifn = arg.dir + '/' + fname + '.json'
        fn = arg.base + '/' + ifn
        with open( fn, 'w' ) as out:
            fc = createFeatureClass( feature[ fname ], featureAttr.get( fname, {} ) )
            fv = createFeatureView( feature[ fname ], featureAttr.get( fname, {} ) )
            # sl = createServiceLayer( feature[ fname ] )

            # out.write( json_out( [ fc, fv, sl ] ) )
            out.write( json_out( [ fc, fv ] ) )
            man.write( ifn + '\n' )
            tool.INFO( 'wrote {}', fn )


