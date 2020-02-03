class CWM( object ):
    def __init__( self, type_name, service, parentsLength, parents, **arg ):
        self.data = {
            "@type": "http://cwm.gov.bc.ca/v1/" + type_name
        }
        self.data.update( arg )

        if not 'name' in self.data:
            raise NameError( '{0} requires a name'.format( type_name ) )

        self._type_name = type_name
        self._service = service

        if len( parents ) < parentsLength:
            raise ReferenceError( '{2} requires {0} parent(s), {1} given'.format( parentsLength, len( parents ), type_name ) )

        self._parents = parents        
        self._parentsLength = parentsLength

    @property
    def type_name( self ):
        return self._type_name

    @property
    def name( self ):
        return self.data[ 'name' ]

    @property
    def service( self ):
        return self._service

    @property
    def parents( self ):
        return self._parents

    @property
    def parentsLength( self ):
        return self._parentsLength

    @property
    def parentName( self ):
        if self.parentsLength == 0:
            return ''

        return '.'.join( [ p.get('name',p.get('id')) for p in self._parents[:self.parentsLength][::-1] ] ) 

    @property
    def fullName( self ):
        p = self.parentName
        if not p:
            return self.name

        return p + '.' + self.name

    @property
    def access( self ):
        return self._type_name + 's/' + self.fullName

    @property
    def create( self ):
        return self._type_name + 's'
    
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def get_arg( **arg ):
    del arg['self']
    return arg

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class queryTemplate( CWM ):
    def __init__( self, parents,
            name, 
            title, 
            mapApplicationId, 
            disclaimer=None, 
            custodianId='5acea04e-9724-4fcc-accb-0e34940da615', 
            isReadonly=False ):

        CWM.__init__( self, self.__class__.__name__, 'sos', 0, **get_arg( **locals() ) )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class queryTemplateLayer( CWM ):
    def __init__( self, parents, 
            name,
            description,
            layerOrder,
            associatedFeatureViewId,
            titleFormat='{0}',
            summaryFormat='{0}',
            isTransactionTimeEnabled=False,
            isValidTimeEnabled=False,
            queryTemplateId=None ):

        if not queryTemplateId:
            queryTemplateId = parents[0]['name']

        CWM.__init__( self, self.__class__.__name__, 'sos', 1, **get_arg( **locals() ) )

    # @property
    # def access( self ):
    #     return 'queryTemplates/' + self.parentName + '/' + self.type_name + 's'

    @property
    def create( self ):
        return 'queryTemplates/' + self.parentName + '/' + self.type_name + 's'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class templateLayerAttribute( CWM ):
    def __init__( self, parents,
            name,
            title,
            attributeOrder,
            description,
            associatedFeatureViewAttributeId,
            queryTemplateLayerId=None ):

        if not queryTemplateLayerId:
            queryTemplateLayerId = parents[1]['name'] + '.' + parents[0]['name']

        CWM.__init__( self, self.__class__.__name__, 'sos', 2, **get_arg( **locals() ) )

    # @property
    # def create( self ):
    #     return 'queryTemplateLayers/' + self.parentName + '/' + self.type_name + 's'

    @property
    def create( self ):
        return 'queryTemplateLayers/' + self.parentName + '/' + self.type_name + 's'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class featureClass( CWM ):
    def __init__( self, parents,
            name,
            description,
            dataSourceId,
            geometryAttrTableName,
            configurationScopeName,
            geometryAttrColumnName='GEOMETRY',
            dataCustodianGuid='0',
            activePeriodStart=None,
            activePeriodEnd=None,
            doesSupportTransactionTime=False,
            doesSupportValidTime=False,
            startTimeAttrTableName=None,
            endTimeAttrTableName=None,
            startTimeAttrColumnName=None,
            endTimeAttrColumnName=None,
            latestTestResult=None,
            sourceTables=None ):

        CWM.__init__( self, self.__class__.__name__, 'cwms', 0, **get_arg( **locals() ) )

    @property
    def access( self ):
        return self.type_name + 'es/' + self.name

    @property
    def create( self ):
        return self.type_name + 'es'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class sourceTable( CWM ):
    def __init__( self, parents,
            name,
            schema='proxy_cwms',
            joinClause=None,
            sourceAttributes=None,
            featureClassId=None ):

        if not featureClassId:
            featureClassId = parents[0]['name']

        CWM.__init__( self, self.__class__.__name__, 'cwms', 1, **get_arg( **locals() ) )

    @property
    def create( self ):
        return 'featureClasses/' + self.parentName + '/' + self.type_name + 's'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class sourceAttribute( CWM ):
    def __init__( self, parents,
            name,
            attributeOrder,
            description,
            attributeTypeCode,
            whereClause=None,
            sourceTableId=None ):

        if not sourceTableId:
            sourceTableId = parents[1]['name'] + '.' + parents[0]['name']

        CWM.__init__( self, self.__class__.__name__, 'cwms', 2, **get_arg( **locals() ) )

    @property
    def create( self ):
        return 'sourceTables/' + self.parentName + '/' + self.type_name + 's'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class featureView( CWM ):
    def __init__( self, parents,
            name,
            description,
            metadataUrl,
            oauth2RenderScopeName,
            oauth2ReportScopeName,
            oauth2ViewScopeName,
            featureViewCategoryId,
            featureClassId,
            isTransactionTimeEnabled=False,
            isValidTimeEnabled=True,
            isSensitive=False,
            featureViewAttributes=None ):

        CWM.__init__( self, self.__class__.__name__, 'cwms', 0, **get_arg( **locals() ) )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class featureViewAttribute( CWM ):
    def __init__( self, parents,
            name,
            title,
            attributeOrder,
            description,
            sourceAttributeId,
            parentId=None ):

        if not parentId:
            parentId = parents[0]['name']

        CWM.__init__( self, self.__class__.__name__, 'cwms', 1, **get_arg( **locals() ) )

    @property
    def create( self ):
        return 'featureViews/' + self.parentName + '/' + self.type_name + 's'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class mapApplication( CWM ):
    def __init__( self, parents,
            name,
            title,
            description,
            locationUrl,
            custodianGuid,
            applicationAcronym,
            oauth2DeveloperScopeName,
            oauth2TesterScopeName,
            oauth2ExecutionScopeName,
            cwmVersion,
            environment ):

        CWM.__init__( self, self.__class__.__name__, 'cwms', 0, **get_arg( **locals() ) )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class map( CWM ):
    def __init__( self, parents,
            name,
            title,
            description,
            properties,
            custodianId,
            mapApplicationId=None,
            defaultBaseLayerId=None ):
            # mapPrintRenderingServiceId=None ):

        if not mapApplicationId:
            mapApplicationId = parents[0]['name']

        CWM.__init__( self, self.__class__.__name__, 'cwms', 1, **get_arg( **locals() ) )

    @property
    def create( self ):
        return 'mapApplications/' + self.parentName + '/' + self.type_name + 's'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class overlay( CWM ):
    def __init__( self, parents,
            name,
            title,
            mapServiceId,
            properties,
            orderInMap,
            typeCode = 'WMS',
            mapId = None ):

        if not mapId:
            mapId = parents[0]['name']

        CWM.__init__( self, self.__class__.__name__, 'cwms', 2, **get_arg( **locals() ) )

    @property
    def create( self ):
        return 'maps/' + self.parentName + '/' + self.type_name + 's'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class styledLayer( CWM ):
    def __init__( self, parents,
            name,
            title,
            properties,
            wmsServiceLayerId,
            wmsLayerStyleId,
            orderInOverlay,
            overlayId = None ):

        if not overlayId:
            overlayId = parents[0]['name']

        CWM.__init__( self, self.__class__.__name__, 'cwms', 3, **get_arg( **locals() ) )

    @property
    def create( self ):
        return 'overlays/' + self.parentName + '/' + self.type_name + 's'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class wmsStyledLayerAttribute( CWM ):
    def __init__( self, parents,
            name,
            title,
            description,
            attributeOrder,
            associatedFeatureViewAttributeId,
            properties,
            wmsStyledLayerId = None ):

        if not wmsStyledLayerId:
            wmsStyledLayerId = parents[0]['name']

        CWM.__init__( self, self.__class__.__name__, 'cwms', 4, **get_arg( **locals() ) )

    @property
    def create( self ):
        return 'styledLayers/' + self.parentName + '/' + self.type_name + 's'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class managedMapServer( CWM ):
    def __init__( self, parents,
            name,
            configurationUserName,
            configurationPassword,
            baseConfigurationUrl ):

        CWM.__init__( self, self.__class__.__name__, 'cwms', 0, **get_arg( **locals() ) )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class managedWMSMapService( CWM ):
    def __init__( self, parents,
            name,
            title,
            description,
            endpointUrl,
            isAvailable,
            hostingMapServerId ):

        CWM.__init__( self, self.__class__.__name__, 'cwms', 0, **get_arg( **locals() ) )

    @property
    def access( self ):
        return 'mapServices/' + self.name

    @property
    def create( self ):
        return 'mapServices'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class managedWMSServiceLayer( CWM ):
    def __init__( self, parents,
            name,
            title,
            description,
            defaultStyleId,
            associatedFeatureViewId,
            associatedManagedWMSServiceId,
            metadataUrl,
            associatedWMSServiceLayerTemplateId = None ):

        CWM.__init__( self, self.__class__.__name__, 'cwms', 0, **get_arg( **locals() ) )

    @property
    def access( self ):
        return 'serviceLayers/' + self.name

    @property
    def create( self ):
        return 'serviceLayers'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# broken
# now managedWMSLayerStyle, parented by ManagedWMSMapService
class WMSLayerStyleResource( CWM ):
    def __init__( self, parents,
            name,
            title,
            definition,
            definitionHash = None ):

        CWM.__init__( self, self.__class__.__name__, 'cwms', 1, **get_arg( **locals() ) )

    @property
    def access( self ):
        return 'wmsLayerStyles/' + self.fullName

    @property
    def create( self ):
        return 'serviceLayers/' + self.parentName + '/wmsLayerStyles'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class oracleDataSource( CWM ): 
    def __init__( self, parents,
            name,
            description,
            accessScopeName,
            configurationScopeName,
            connectionPoolSize,
            instance,
            password,
            port,
            server,
            userid ):

        CWM.__init__( self, self.__class__.__name__, 'cwms', 0, **get_arg( **locals() ) )

    @property
    def access( self ):
        return 'dataSources/' + self.fullName

    @property
    def create( self ):
        return 'dataSources'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class featureViewCategory( CWM ): 
    def __init__( self, parents,
            name,
            description ):

        CWM.__init__( self, self.__class__.__name__, 'cwms', 0, **get_arg( **locals() ) )

    @property
    def access( self ):
        return 'featureViewCategories/' + self.fullName

    @property
    def create( self ):
        return 'featureViewCategories'
