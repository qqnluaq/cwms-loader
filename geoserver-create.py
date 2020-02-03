#!/usr/bin/python

import sys
import csv
import json
from collections import OrderedDict
import argparse
import json
import traceback
import requests
from prompt import prompt 
import tool 

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

tool.add_argument( '--style-dir', '-s',  
    help    = 'directory containing SLDs',
    dest    = 'styleDir',
    default = 'styles' )

tool.add_argument( '--test', '-t',  
    help    = 'test existance of layers',
    action  = 'store_true' )

args = tool.args()

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

env = tool.import_environment()
if not env:
    tool.INFO( "Could not load environment {0}", environment_name() )
    exit(1)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def make_request( method, url, **kwargs ):
    tool.VERBOSE( method.upper() + ' ' + url )
    # tool.VERBOSE( json.dumps( kwargs['headers'], sort_keys=True, indent=2, separators=(',', ': ') ) ) 
    if 'data' in kwargs:
        tool.VERBOSE( kwargs['data'] ) 

    if not 'auth' in kwargs:
        kwargs['auth'] = ( env.gsUser(), env.gsPassword() )

    r = getattr( requests, method.lower() )( url, **kwargs )

    res = True if r.ok else ( None if r.status_code >= 500 else False )

    tool.VERBOSE( 'status {0}: {1}', r.status_code, res )

    if r.headers.get('content-type','').endswith('png'):
        tool.VERBOSE( '<binary data>' )
    else:
        tool.VERBOSE( r.text )

    return res, r 


def test_feature_type( layer ):
    url = '{0}/rest/workspaces/{1}/datastores/{2}/featuretypes/{3}'.format( env.gsHost(), layer['workspace'], layer['datastore'], layer['name'] )

    r,_ = make_request( 'get', url )

    return r != None


def delete_feature_type( layer ):
    url = '{0}/rest/workspaces/{1}/datastores/{2}/featuretypes/{3}?recurse=true'.format( env.gsHost(), layer['workspace'], layer['datastore'], layer['name'] )

    r,_ = make_request( 'delete', url )

    return r != None


def delete_style( layer ):
    url = '{0}/rest/styles/{1}'.format( env.gsHost(), layer['name'] )

    r,_ = make_request( 'delete', url )

    return r != None


def create_feature_type( layer ):
    url = '{0}/rest/workspaces/{1}/datastores/{2}/featuretypes'.format( env.gsHost(), layer['workspace'], layer['datastore'] )

    template = '''
    <featureType>
     <name>{0}</name>
     <nativeName>{0}</nativeName>
     <title>{1}</title>
     <srs>EPSG:3005</srs>
     <nativeBoundingBox>
      <minx>200000.0</minx>
      <maxx>1900000.0</maxx>
      <miny>300000.0</miny>
      <maxy>1800000.0</maxy>
     </nativeBoundingBox>
    </featureType>
    '''

    data = template.format( layer['name'], layer['title'] )
    r,_ = make_request( 'post', url, headers={ 'Content-type': 'text/xml' }, data=data )

    return r


def create_style( layer ):
    url = '{0}/rest/styles'.format( env.gsHost() )

    template = '''
    <style>
     <name>{0}</name>
     <filename>{1}.sld</filename>
    </style>    
    '''

    data = template.format( layer['name'], layer['styleName'] )
    r,_ = make_request( 'post', url, headers={ 'Content-type': 'text/xml' }, data=data )

    if not r:
        return False

    url = '{0}/rest/styles/{1}'.format( env.gsHost(), layer['styleName'] )

    fh = None
    try:
        fn = '{0}/{1}.SLD'.format( args.styleDir, layer['styleName'] )
        fh = open( fn, 'rb' )
        tool.VERBOSE( 'using SLD {0}', fn )
    except:
        tool.INFO( '{0} missing', fn )
        return False

    template = fh.read()
    fh.close()

    data = template.format( layer['name'], layer['title'] )
    r,_ = make_request( 'put', url, headers={ 'Content-type': 'application/vnd.ogc.sld+xml' }, data=data )

    if not r:
        return False

    url = '{0}/rest/layers/{1}'.format( env.gsHost(), layer['name'] )

    template = '''
    <layer>
     <defaultStyle>
      <name>{0}</name>
     </defaultStyle>
    </layer>
    '''

    data = template.format( layer['styleName'] )
    r,_ = make_request( 'put', url, headers={ 'Content-type': 'text/xml' }, data=data )

    return r


def test_layer_wms( layer ):
    url = '{0}/{1}/wms?LAYERS={2}&SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&FORMAT=image%2Fpng&SRS=EPSG%3A3857&BBOX=-17092373,4020871,-11082588,10669057&WIDTH=1228&HEIGHT=1359'.format( env.gsHost(), layer['workspace'], layer['name'] )

    r,res = make_request( 'get', url, auth=None )

    return res.headers['content-type'].endswith( 'png' )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def test_layer( layer ):
    ok = test_feature_type( layer )
    if not ok:
        return ( False, 'Unable to find layer' )

    ok = test_layer_wms( layer )
    if not ok:
        return ( False, 'Unable get layer with WMS GetMap' )

    return ( True, None )


def publish_layer( layer ):
    ok = delete_feature_type( layer )
    if not ok:
        return ( False, 'Unable to delete featuretype' )

    ok = delete_style( layer )
    if not ok:
        return ( False, 'Unable to delete style' )

    ok = create_feature_type( layer )
    if not ok:
        return ( False, 'Unable to create featuretype' )

    ok = create_style( layer )
    if not ok:
        return ( False, 'Unable to create style' )

    ok = test_layer_wms( layer )
    if not ok:
        return ( False, 'Unable get layer with WMS GetMap' )

    return ( True, None )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

status = []
total_layers = 0
total_failed = 0
for name in tool.get_filenames():
    layer_status = OrderedDict([('filename',name)])
    status.append( layer_status )
    layers = []    
    try:
        tool.VERBOSE( 'reading {0}...', name )
        with open( name, 'rb') as file:
            layers = json.loads( file.read() )

    except Exception as e:
        tool.VERBOSE( traceback.format_exc() )
        layer_status['error']=str(e)

    layer_status['layers'] = layers
    failed = 0
    for layer in layers:
        try:
            total_layers += 1

            if args.test:
                tool.INFO( 'Testing {0}: \\', layer['name'] ) 
                layer['ok'], layer['error'] = test_layer( layer )
            else:
                tool.INFO( 'Publishing {0}: \\', layer['name'] ) 
                layer['ok'], layer['error'] = publish_layer( layer )

            if layer['ok']:
                tool.INFO( 'Successful' )
            else:
                tool.INFO( 'Failed, {0}', layer['error'] )
                failed += 1
                if args.stop:
                    break

        except Exception as e:
            layer['error'] = str(e)
            tool.INFO( 'Failed, {0}', layer['error'] )
            tool.VERBOSE( traceback.format_exc() )
            failed += 1
            if args.stop:
                break

    total_failed += failed

    if failed and args.stop:
        break

if total_failed < total_layers:
    print 'Succeeded'
    for s in status:
        for ly in s.get('layers',[]):
            if ly.get('ok'):
                print '  {0}: {1}'.format( s['filename'], ly['name'] )
    print 

if total_failed > 0:
    print 'Failed'
    for s in status:
        for ly in s.get('layers',[]):
            if not ly.get('ok'):
                print '  {0}: {1} ({2})'.format( s['filename'], ly['name'], ly.get('error', '') )
    print 

if args.test:
    print '{0}/{1} layer(s) test OK'.format( total_layers - total_failed, total_layers )
else:
    print '{0}/{1} layer(s) published'.format( total_layers - total_failed, total_layers )

exit( total_failed )