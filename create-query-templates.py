#!/usr/bin/python

import csv
import json
from collections import OrderedDict
import argparse
import traceback
import os
import tables
import tool

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# name, title
tool.add_argument( '--query-templates', '-q', 
    help    = 'CSV containing query templates',
    dest    = 'qt',
    default = tables.maps )

# templateName, name, description,nameFormat,summaryFormat,featureName,order
tool.add_argument( '--query-template-layers', '-l', 
    help    = 'CSV containing query template layers',
    dest    = 'qtl',
    default = tables.queryTemplateLayers )

# templateLayerName, name, title, order, description, featureAttributeName
tool.add_argument( '--query-template-layer-attributes', '-a', 
    help    = 'CSV containing query template layer attributes',
    dest    = 'qtla',
    default = tables.queryTemplateLayerAttributes )

tool.add_argument( '--out-dir', '-d', 
    help    = 'name of directory to receive files',
    dest    = 'dir',
    default = 'query-templates' )

tool.add_argument( '--manifest', '-m', 
    help    = 'name of file to receive manifest of files created',
    dest    = 'manifest',
    default = 'query-templates.manifest' )

tool.add_argument( '--constants', '-c', 
    help    = 'JSON containing constant columns',
    dest    = 'constants',
    default = 'constants.json' )

tool.add_argument( '--base-dir', '-b', 
    help    = 'name of directory to work in',
    dest    = 'base',
    default = '.' )

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
 
queryTemplate = OrderedDict()
const = constant.get( 'queryTemplate', {} )
for qt in tables.fetchRows( arg.qt ):
    if qt['name'] in queryTemplate:
        print 'query template '+qt['name']+' duplicated, skipping'
        continue

    for k in const:
        if not k in qt or not qt[k]:
            qt = dict( qt )
            qt[ k ] = const[ k ]

    # print qt
    queryTemplate[ qt['name'] ] = qt

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

queryTemplateLayer = OrderedDict()
queryTemplateLayerOrder = {
    "All":            0,
    "FishWildlife":   146,
    "Forest":         291,
    "Land":           435,
    "Mining":         579,
    "OilGas":         724,
    "Water":          869
}
const = constant.get( 'queryTemplateLayer', {} )
for qtl in tables.fetchRows( arg.qtl ):
    qtl = dict( qtl )

    if not qtl['templateName'] in queryTemplate:
        print 'query template '+qtl['templateName']+' not found in '+arg.qt+', skipping'
        continue

    if not qtl['templateName'] in queryTemplateLayer:
        queryTemplateLayer[ qtl['templateName'] ] = {}

    if qtl['name'] in queryTemplateLayer[ qtl['templateName'] ]:
        print 'query template layer '+qtl['templateName']+'.'+qtl['name']+' duplicated, skipping'
        continue

    for k in const:
        if not k in qtl or not qtl[k]:
            qtl[ k ] = const[ k ]

    if not qtl.get( 'order' ):
        qtl['order'] = queryTemplateLayerOrder[ qtl['templateName'] ]
        queryTemplateLayerOrder[ qtl['templateName'] ] += 1

    # print qtl
    queryTemplateLayer[ qtl['templateName'] ][ qtl['name'] ] = qtl

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

queryTemplateLayerAttribute = OrderedDict()
const = constant.get( 'queryTemplateLayerAttribute', {} )
for qtla in tables.fetchRows( arg.qtla ):
    if not qtla['templateName'] in queryTemplate:
        print 'query template '+qtla['templateName']+' not found in '+arg.qt+', skipping'
        continue

    if not qtla['templateLayerName'] in queryTemplateLayer[ qtla['templateName'] ]:
        print 'query template layer '+qtla['templateName']+'.'+qtla['templateLayerName']+' not found in '+arg.qtl+', skipping'
        continue

    if not qtla['templateName'] in queryTemplateLayerAttribute:
        queryTemplateLayerAttribute[ qtla['templateName'] ] = {}

    if not qtla['templateLayerName'] in queryTemplateLayerAttribute[ qtla['templateName'] ]:
        queryTemplateLayerAttribute[ qtla['templateName'] ][ qtla['templateLayerName'] ] = {}

    if qtla['name'] in queryTemplateLayerAttribute[ qtla['templateName'] ][ qtla['templateLayerName'] ]:
        print 'query template layer attribute '+qtla['templateName']+'.'+qtla['templateLayerName']+'.'+qtla['name']+' duplicated, skipping'
        continue

    for k in const:
        if not k in qtla or not qtla[k]:
            qtla = dict( qtla )
            qtla[ k ] = const[ k ]

    # print qtla
    queryTemplateLayerAttribute[ qtla['templateName'] ][ qtla['templateLayerName'] ][ qtla['name'] ] = qtla

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def createQueryTemplate( qt, qtl, qtla ):
    return OrderedDict( [
        ( "@mode",            "create"               ),
        ( "@type",            "queryTemplate"        ),
        ( "name",             qt['name']             ),
        ( "title",            qt['title']            ),
        ( "disclaimer",       qt['disclaimer']       ),
        ( "mapApplicationId", qt['mapApplicationId'] ),
        ( "@children",        [ createQueryTemplateLayer( qtl[ qtln ], qtla.get( qtln, {} ) ) for qtln in sorted( qtl, key=lambda k:int(qtl[k]['order']) ) ] )
    ] )

def createQueryTemplateLayer( qtName, qtl, qtla ):
    qtlas = [ qtla[ qtlan ] for qtlan in sorted( qtla, key=lambda k:int(qtla[k]['order']) ) ]

    bizIndex = [ ( i, qtlas[i]['title'] ) for i in range(len(qtlas)) if int(qtlas[i]['businessKey']) == 1 ]

    # if not 'titleFormat' in qtl or not qtl['titleFormat']:
    #     if bizIndex:
    #         qtl['titleFormat'] = ' - '.join( [ i[1] for i in bizIndex ] )
    #     else:
    #         qtl['titleFormat'] = 'No businessKey'

    if not 'summaryFormat' in qtl or not qtl['summaryFormat']:
        if bizIndex:
            qtl['summaryFormat'] = ' / '.join( [ '{1}: {{{0}}}'.format( i[0], i[1] ) for i in bizIndex ] )
        else:
            qtl['summaryFormat'] = 'No Business Key'

    return OrderedDict( [
        ( '@parents',                [{'name': qtName}]   ),
        ( '@mode',                   'recreate'           ),
        ( "@type",                   "queryTemplateLayer" ),
        ( "name",                    qtl['name']          ),
        ( "description",             qtl['description']   ),
        ( "titleFormat",             qtl['titleFormat']    ),
        ( "summaryFormat",           qtl['summaryFormat'] ),
        ( "associatedFeatureViewId", qtl['featureName']   ),
        ( "layerOrder",              int(qtl['order'])    ),
        ( "@children",               [ createQueryTemplateLayerAttribute( a, qtl['featureName'] ) for a in qtlas ] )
    ] )

def createQueryTemplateLayerAttribute( qtla, featureName ):
    return OrderedDict( [
        ( '@mode',                            'recreate'               ),
        ( "@type",                            "templateLayerAttribute" ),
        ( "name",                             qtla['name']             ),
        ( "title",                            qtla['title']            ),
        ( "attributeOrder",                   int(qtla['order'])       ),
        ( "description",                      qtla['description']      ),
        ( "associatedFeatureViewAttributeId", featureName + '.' + qtla['featureAttributeName'] )
    ] )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

if not os.path.isdir( arg.base + '/' + arg.dir ):
    os.makedirs( arg.base + '/' + arg.dir )

def json_out( data ):
    return json.dumps( data, sort_keys=False, indent=2, separators=(',', ': ') )

ln = {}
for qtName in queryTemplate:
    for lyName in queryTemplateLayer.get( qtName, {} ):
        ln[ lyName ] = True

layerNames = sorted(ln.keys())
templateNames = sorted(queryTemplate.keys())

with open( arg.base + '/' + arg.manifest, 'w' ) as man:
    for lyName in layerNames:
        for qtName in templateNames:
            if not lyName in queryTemplateLayer.get( qtName, {} ):
                continue

            ifn = arg.dir + '/' + qtName + '-' + lyName + '.json'
            fn = arg.base + '/' + ifn
            with open( fn, 'w' ) as out:
                qtl = createQueryTemplateLayer( qtName, queryTemplateLayer.get( qtName, {} ).get( lyName, {} ), queryTemplateLayerAttribute.get( qtName, {} ).get( lyName, {} ) )

                out.write( json_out( [ qtl ] ) )
            
            man.write( ifn + '\n' )
            tool.INFO( 'wrote {}', fn )

