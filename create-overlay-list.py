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

# name
tool.add_argument( '--maps', '-m', 
    help    = 'CSV containing maps',
    dest    = 'maps',
    default = tables.maps )

# mapName, title, id, layerType, url, transparent, singleTile, opacity, expanded, visibility
tool.add_argument( '--overlays', '-o', 
    help    = 'CSV containing overlays',
    dest    = 'overlays',
    default = tables.mapOverlays )

# mapName, overlayId, title, id, layers, styles, url, geometryAttribute, titleAttribute, identifiable, selectable, visibility, max_scale, 
tool.add_argument( '--layers', '-l', 
    help    = 'CSV containing layers',
    dest    = 'layers',
    default = tables.mapOverlayLayers )

# mapName, overlayId, layerId, name, title
tool.add_argument( '--attributes', '-a', 
    help    = 'CSV containing attributes',
    dest    = 'attributes',
    default = tables.mapOverlayLayerAttributes )

tool.add_argument( '--base-dir', '-b', 
    help    = 'name of directory to work in',
    dest    = 'base',
    default = '.' )

tool.add_argument( '--out-dir', '-d', 
    help    = 'name of directory to receive files',
    dest    = 'dir',
    default = 'overlays' )

tool.add_argument( '--constants', '-c', 
    help    = 'JSON containing constant columns',
    dest    = 'constants',
    default = 'constants.json' )

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
 
mapObj = OrderedDict()
const = constant.get( 'map', {} )
for m in tables.fetchRows( arg.maps ):
    mapObj[ m['name'] ] = m

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 
overlay = OrderedDict()
const = constant.get( 'mapOverlay', {} )
for ov in tables.fetchRows( arg.overlays ):
    for k in const:
        if not k in ov or not ov[k]:
            ov = dict( ov )
            ov[ k ] = const[ k ]

    # print ov
    overlay.setdefault( ov['mapName'], OrderedDict() )[ ov['id'] ] = ov

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 
layer = OrderedDict()
const = constant.get( 'mapOverlayLayer', {} )
for ly in tables.fetchRows( arg.layers ):
    for k in const:
        if not k in ly or not ly[k]:
            ly = dict( ly )
            ly[ k ] = const[ k ]

    # print ly
    layer.setdefault( ly['mapName'], OrderedDict() ).setdefault( ly['overlayId'], OrderedDict() )[ ly['id'] ] = ly

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 
attribute = OrderedDict()
const = constant.get( 'mapOverlayLayerAttribute', {} )
for at in tables.fetchRows( arg.attributes ):
    for k in const:
        if not k in at or not at[k]:
            at = dict( at )
            at[ k ] = const[ k ]

    # print at
    attribute.setdefault( at['mapName'], OrderedDict() ).setdefault( at['overlayId'], OrderedDict() ).setdefault( at['layerId'], OrderedDict() )[ at['name'] ] = at

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def createOverlay( ov, lyr, attr ):
    if not 'visibility' in ov:
        ov['visibility'] = any( [ bool(int(lyr[n]['visibility'])) for n in lyr ] )
        
    return OrderedDict( [
        ( "title",       ov["title"]       ),
        ( "id",          ov["id"]          ),
        ( "layerType",   ov["layerType"]   ),
        ( "url",         constant.get( 'overlayUrl', {} ).get( ov["url"], ov["url"] ) ),
        ( "transparent", bool(int(ov["transparent"])) ),
        ( "singleTile",  bool(int(ov["singleTile"]))  ),
        ( "opacity",     float(ov["opacity"])     ),
        ( "expanded",    bool(int(ov["expanded"]))    ),
        ( "visibility",  bool(int(ov["visibility"]))  ),
        ( "layer",       [ createLayer( lyr[ lyrn ], attr.get( lyrn, {} ) ) for lyrn in lyr ] ) 
    ] )

def createLayer( lyr, attr ):
    d = OrderedDict( [
        ( "title",             lyr["title"]             ),
        ( "id",                lyr["id"]                ),
        ( "layers",            lyr["layers"]            ),
        ( "styles",            lyr["styles"]            ),
        # ( "url",               lyr["url"]               ),
        ( "geometryAttribute", lyr["geometryAttribute"] ),
        # ( "titleAttribute",    lyr["titleAttribute"]    ),
        ( "max_scale",         int(lyr.get("max_scale")) ),
        ( "identifiable",      bool(int(lyr["identifiable"]))      ),
        ( "selectable",        bool(int(lyr["selectable"]))        ),
        ( "visibility",        bool(int(lyr["visibility"]))        ),
        ( "legend",            { "visible": True, "useLayerTitle": True } ),
        ( "attributes",        [ createAttribute( attr[ attrn ] ) for attrn in attr ] ) 
    ] )

    if not d['max_scale']:
        del d['max_scale']

    if d['geometryAttribute'] == 'NOTHING':
        del d['geometryAttribute']

    return d

def createAttribute( attr ):
    return OrderedDict( [
        ( "name",  attr["name"]  ),
        ( "title", attr["title"] ),
    ] )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def json_out( data ):
    return json.dumps( data, sort_keys=False, indent=2, separators=(',', ': ') )

if not os.path.isdir( arg.base + '/' + arg.dir ):
    os.makedirs( arg.base + '/' + arg.dir )

for mapN in mapObj:
    mOv = overlay.get( mapN )
    if not mOv: 
        continue

    fn = arg.base + '/' + arg.dir + '/' + mapN + '.json'
    with open( fn, 'w' ) as out:
        try:
            mLy = layer.get( mapN, {} )
            mAt = attribute.get( mapN, {} )
            ovs = [ createOverlay( mOv[ ovId ], mLy.get( ovId, {} ), mAt.get( ovId, {} ) ) for ovId in mOv ] # sorted( mOv, key = lambda k: mOv[k]['title'] ) ]
            out.write( json_out( { 'overlays': ovs } ) )
            tool.INFO( '{} written', fn ) 
        except:
            tool.INFO( 'Map {} failed', mapN ) 
            print traceback.format_exc()

