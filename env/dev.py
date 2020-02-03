from prompt import prompt 

#
# Host name of service
# 

@prompt( 'Service Base URL Hostname' )
def serviceBaseHost():
	return 'd1api.vividsolutions.com'

#
# OAuth configuration
# 

@prompt( 'OAuth Base URL Hostname' )
def oauthBaseHost():
	return serviceBaseHost()

def oauthBase():
	return 'https://' + oauthBaseHost() + '/oauth2/v1/'

def oauthParams( scope ):
	return {
		'disableDeveloperFilter': False,
		'grant_type':             'client_credentials',
		'scope':                  scope,
		'redirect_uri':           'http://www.redirecturi.com'
	}

#
# CWMS specific authorization
# 

@prompt( 'OAuth User for CWMS Service' )
def cwmsUser():
	return 'CWMS-REST'

@prompt( 'OAuth Password for CWMS Service' )
def cwmsPassword():
	return 'password'

@prompt( 'OAuth token service scope for CWMS Service' )
def cwmsScope():
	return 'CWMS-REST.*'

def cwmsBase():
	return 'https://' + serviceBaseHost() + '/cwm-cwms-api/v1/'

#
# SOS specific authorization
# 

@prompt( 'OAuth User for SOS Service' )
def sosUser():
	return 'SOS-REST'

@prompt( 'OAuth Password for SOS Service' )
def sosPassword():
	return ''

@prompt( 'OAuth token service scope for SOS Service' )
def sosScope():
	return 'SOS-REST.*'

def sosBase():
	return 'https://' + serviceBaseHost() + '/cwm-sos-api/v1/'

#
# Geoserver
# 

def gsHost():
	return 'http://atari:8080/geoserver'

def gsUser():
	return 'admin'

def gsPassword():
	return ''

