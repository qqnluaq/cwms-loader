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

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

TEST = 1
DELETE = 2
CREATE = 3
RECREATE = 4
UPDATE = 5
object_mode = { 'test':TEST, 'delete':DELETE, 'create':CREATE, 'recreate':RECREATE, 'update':UPDATE }

tool.add_argument( '--force',  
    help    = 'force delete of object if it appears to not exist', 
    action  = 'store_true' )

tool.add_argument( '--mode', '-m',  
    help    = 'mode for handling objects, overriding @mode directive on object',
    choices = object_mode.keys() )

tool.add_argument( '--default-mode', '-d',  
    help    = 'mode for handling objects, with no @mode directive',
    choices = object_mode.keys(),
    dest    = 'defaultMode',
    default = 'test' )

tool.add_argument( '--report-filename', '-r',  
    help    = 'send report to this file. Set by default when using @filename',
    dest    = 'reportFilename' )

args = tool.args()

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

env = tool.import_environment()
if not env:
    tool.tool.INFO( "Could not load environment {0}", environment_name() )
    exit(1)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class Requester:
    def __init__( self, user, password, scope, baseURL ):
        self._user = user
        self._password = password
        self._scope = scope
        self._baseURL = baseURL
        self.headers = None

    @property
    def user( self ):
        if callable( self._user ):
            return self._user()
    
        return self._user

    @property
    def password( self ):
        if callable( self._password ):
            return self._password()
    
        return self._password

    @property
    def scope( self ):
        if callable( self._scope ):
            return self._scope()
    
        return self._scope

    @property
    def baseURL( self ):
        if callable( self._baseURL ):
            return self._baseURL()
    
        return self._baseURL


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

        if args.dryrun:
            return ( 'dryrun', '', {} )

        r = getattr( requests, method.lower() )( url, **kwargs )

        res = True if r.ok else ( None if r.status_code >= 500 else False )
        ver = eval( r.headers.get( 'ETag', '""') )

        tool.VERBOSE( 'status {0}: {1}', r.status_code, res )
        try:
            blob = json.loads( r.text )
            if 'links' in blob:
                del blob['links']
            tool.VERBOSE( json.dumps( blob, sort_keys=True, indent=2, separators=(',', ': ') ) )
            return ( res, ver, blob )

        except:
            tool.VERBOSE( r.text )
            return ( res, ver, r.text )

    def element_exists( self, path ):
        ok, ver, data = self._make_request( 'get', self.baseURL + path, 
            headers = self.get_headers()
        )
        
        try:
            return ( ok, ver, data['id'] )
        except:
            return ( ok, ver, None )

    def delete_element( self, path, ver ):
        ok, newVer, data = self._make_request( 'delete', self.baseURL + path, 
            headers = self.get_headers(
                **{ 'If-Match': '"' + ver + '"' }
            )
        )
        
        return ( ok, newVer, None )

    def load_element( self, path, element ):
        ok, ver, data = self._make_request( 'post', self.baseURL + path, 
            data    = json.dumps( element ),
            headers = self.get_headers()
        )

        try:
            return ( ok, ver, data['id'] )
        except: 
            return ( ok, ver, None )

    def update_element( self, path, ver, id, element ):
        element['id'] = id
        ok, newVer, data = self._make_request( 'put', self.baseURL + path, 
            data    = json.dumps( element ),
            headers = self.get_headers(
                **{ 'If-Match': '"' + ver + '"' }
            )
        )

        try:
            return ( ok, newVer, data['id'] )
        except: 
            return ( ok, newVer, None )

service = {
    'cwms': Requester( env.cwmsUser, env.cwmsPassword, env.cwmsScope, env.cwmsBase ),
    'sos':  Requester( env.sosUser,  env.sosPassword,  env.sosScope,  env.sosBase  )
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def expand_object( parents, obj, index ):
    obj_parents = obj.get( '@parents', [] )
    obj_parents.extend( parents )
    parents = obj_parents
    
    context = '{0} [{1}]'.format( '.'.join( [o['name'] for o in parents[::-1]] ), index )

    obj_type = obj.get( '@type', False )
    if not obj_type:
        tool.INFO( 'object at {0} is missing @type', context )
        return ( None, None )

    if obj_type.rfind('/') >= 0:
        obj_type = obj_type[ obj_type.rfind('/')+1 : ]

    context = '[{0}: {1}]'.format( obj_type, context )

    template_param = { k:obj[ k ] for k in obj if k[ 0 ] != '@' and k != 'id' }
    template_param[ 'parents' ] = parents

    try:
        cwm_cls = getattr( cwm, obj_type )
        argspec = inspect.getargspec( cwm_cls.__init__ )
        required_args = argspec.args[ 1:( len(argspec.args) - len(argspec.defaults or []) ) ] 
        
        cwm_obj = cwm_cls( **template_param )
        # print cwm_obj.data
        return ( parents, cwm_obj )

    except TypeError as e:
        # print 'object at ' + str(context) + ' is missing property.\n  Provided: ' + str(template_param.keys()) + '\n  Needed: ' + str(required_args)
        tool.INFO( 'object {0} has wrong properties\n  Provided: {1}\n  Needed: {2}', context, sorted(template_param.keys()), sorted(required_args) )
        tool.VERBOSE( traceback.format_exc() )
        return ( None, None )

    except Exception as e:
        tool.INFO( 'object {0} failed: {1}', context, e )
        tool.VERBOSE( traceback.format_exc() )
        return ( None, None )



def process_object( obj_parents, obj_param, status, index ):   
    parents, cwm_obj = expand_object( obj_parents, obj_param, index )
    if not cwm_obj:
        return ( 0, False )

    obj = OrderedDict()
    detail = OrderedDict()

    preDelete = obj_param.get( '@preDelete', [] )
    obj['mode'] = args.mode or obj_param.get( '@mode', args.defaultMode )
    mode = object_mode.get( obj['mode'], 0 )

    data = cwm_obj.data

    detail['type'] = cwm_obj.type_name
    detail['name'] = data['name']
    detail['parents'] = [ o['name'] for o in parents[::-1] ]

    obj['obj'] = "{0}: {1}".format( detail['type'], '.'.join( detail['parents'] + [detail['name']] ) )

    if not mode:
        tool.INFO( "mode {0} not valid for {1}", obj['mode'], obj['obj'] )
        return ( 0, False )

    if not obj_param.get( '@enabled', True ):
        tool.INFO( "{0} not enabled, skipping", obj['obj'] )
        return ( 0, False )
       
    status.append( obj )

    svc = service[ cwm_obj.service ]

    detail['exists'], detail['ver'], detail['id'] = svc.element_exists( cwm_obj.access )

    if mode == TEST:
        if detail['exists']:
            tool.VERBOSE( obj['obj'] + ' exists' )
        elif detail['exists'] == False:
            tool.VERBOSE( obj['obj'] + ' missing' )
        else:
            tool.VERBOSE( obj['obj'] + ' unknown' )

        obj['successful'] = detail['exists'] == True

    if preDelete and ( mode == DELETE or mode == RECREATE ):
        detail['preDeleted'] = {}
        for pd in preDelete:
            s, u = pd
            pd_exists, pd_ver, pd_id = service[s].element_exists( u )
            if pd_exists != False or args.force:
                detail['preDeleted'][u],_,_ = service[s].delete_element( u, pd_ver )

                # if detail['preDeleted'][pd]:
                #     tool.VERBOSE( obj['obj'] +' pre-deleted '+pd )

                status.append( OrderedDict([
                    ( 'mode',       'predelete' ),
                    ( 'obj',        u          ),
                    ( 'successful', detail['preDeleted'][u] )
                ]))

    if mode == DELETE:
        if args.force or detail['exists'] != False:
            detail['deleted'],_,_ = svc.delete_element( cwm_obj.access, detail['ver'] )

            # if detail['deleted']:
            #     tool.VERBOSE( obj['obj'] + ' deleted' )

            obj['successful'] = detail['deleted']               
        else:
            obj['successful'] = True               

    if mode == RECREATE:
        if args.force or detail['exists'] != False:
            detail['deleted'],_,_ = svc.delete_element( cwm_obj.access, detail['ver'] )

            # if detail['deleted']:
            #     tool.VERBOSE( obj['obj'] + ' deleted' )

        if detail['exists'] == False or detail['deleted'] or args.force:
            detail['created'], detail['newVer'], detail['id'] = svc.load_element( cwm_obj.create, data )
            
            # if detail['created']:
            #     tool.VERBOSE( obj['obj'] + ' created' )

            obj['successful'] = detail['created']
        else:
            obj['successful'] = False

    if mode == CREATE:
        if detail['exists'] == False:
            detail['created'], detail['newVer'], detail['id'] = svc.load_element( cwm_obj.create, data )
            
            # if detail['created']:
            #     tool.VERBOSE( obj['obj'] + ' created' )

            obj['successful'] = detail['created']
        else:
            obj['successful'] = detail['exists'] == True 

    if mode == UPDATE:
        if detail['exists']:
            detail['updated'], detail['newVer'], detail['id'] = svc.update_element( cwm_obj.access, detail['ver'], detail['id'], data )
            
            # if detail['updated']:
            #     tool.VERBOSE( obj['obj'] + ' updated' )

            obj['successful'] = detail['updated']
        elif detail['exists'] == False:
            detail['created'], detail['newVer'], detail['id'] = svc.load_element( cwm_obj.create, data )
            
            # if detail['created']:
            #     tool.VERBOSE( obj['obj'] + ' created' )

            obj['successful'] = detail['created']
        else:
            obj['successful'] = False

    tool.STATUS( '({1}) {0}', obj['obj'], 'OK' if obj['successful'] else 'FAIL' )
    tool.VERBOSE()

    if args.stop and not obj['successful']: 
        return ( 1, False )

    count = 0
    if obj_param.get( '@children' ):
        parent = {
            'name': detail[ 'name' ],
            'id': detail[ 'id' ]
        }
        c, ok = process_child_objects( [ parent ] + parents, obj_param['@children'], status )
        count += c

        if args.stop and not ok:
            return ( 1 + count, False )

    return ( 1 + count, True )


def process_child_objects( parents, objs, status ):
    i = 0
    count = 0
    for obj in objs:
        c, ok = process_object( parents, obj, status, i )
        count += c

        if args.stop and not ok:
            return ( count, False )

        i += 1

    return ( count, True )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

status = []
for name in tool.get_filenames():
    file_status = OrderedDict()
    file_status[ 'filename' ] = name
    file_status[ 'objects' ] = []
    status.append( file_status )

    objects = []
    try:
        tool.INFO( 'reading {0}... \\', name )
        with open( name, 'rb') as file:
            objects = json.loads( file.read() )

    except Exception as e:
        tool.INFO( 'failed' )
        tool.VERBOSE( traceback.format_exc() )
        tool.INFO()

        file_status[ 'error' ] = str(e)
        continue

    tool.VERBOSE()
    file_status[ 'count' ], ok = process_child_objects( [], objects, file_status[ 'objects' ] )
    tool.STATUS( '' )

    tool.INFO( 'processed {0} object(s)\n', file_status[ 'count' ] )

    if args.stop and not ok:
        break

if not status:
    exit(0)

# if args.verbose:
#     print json.dumps( status, sort_keys=False, indent=2, separators=(',', ': ') )
#     print '---'

total_objects = 0
total_fails = 0
fail_files = []
files = []
for s in status:
    fails = 0
    out = OrderedDict()
    files.append( out )

    out['filename'] = s['filename']
    if 'error' in s:
        out['error'] = s['error']
        continue

    f = []
    g = []
    for so in s['objects']:
        total_objects += 1
        if so['successful']:
            g.append( '{0} {1}'.format( so['mode'].upper(), so['obj'] ) )
        else:
            fails += 1
            f.append( '{0} {1}'.format( so['mode'].upper(), so['obj'] ) )

    if g:
        out['successful'] = g

    if f:
        out['failures'] = f
        out['failed'] = fails
        fail_files.append( s['filename'] )

    total_fails += fails
    out['succeeded'] = s.get( 'count', 0 ) - fails

summary = OrderedDict([
    ( 'failed files', fail_files ),
    ( 'total objects', total_objects ),
    ( 'failed objects', total_fails )
])

files.append( summary )

reportFilename = args.reportFilename
if not reportFilename and len( tool.get_filenames() ) > 1:
    reportFilename = args.filename[1:] + '.report'

if reportFilename:
    try:
        with open( reportFilename, 'w' ) as rpt:
            rpt.write( json.dumps( files, sort_keys=False, indent=2, separators=(',', ': ') ) )
            rpt.write( '\n' )
        tool.INFO( 'report output to {0}', reportFilename )
    
    except Exception as e:
        tool.INFO( 'failed to write to {0}: {1}', reportFilename, e)

    print json.dumps( summary, sort_keys=False, indent=2, separators=(',', ': ') )

else:
    print json.dumps( files, sort_keys=False, indent=2, separators=(',', ': ') )
