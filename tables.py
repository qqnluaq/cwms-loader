import csvDB;

db = csvDB.DB()

def fetchRows( obj ):
    if isinstance( obj, csvDB.TableBase ):
        return obj.getRows()

    if isinstance( obj, basestring ):
        return db.makeCsv( inputs=obj ).getRows()

    raise 'cant handle {}'.format( obj )

#
# Source datasets
#

tblConfigMapLayers = db.makeCsv( inputs='source/tblConfigMapLayers.csv' )

tblConfigMapAttributes = db.makeCsv( inputs='source/tblConfigMapAttributes.csv' )

dataSources = db.makeCsv( inputs='source/dataSources.csv' )

formats = db.makeCsv( inputs='source/formats.csv' )

columnTypes = db.makeCsv( inputs='source/columnTypes.csv' )

layerCategories = db.makeCsv( inputs='source/layerCategories.csv' )

overlayUrls = db.makeCsv( inputs='source/overlayUrls.csv' )

overlayLayers = db.makeCsv( inputs='source/overlayLayers.csv' )

maps = db.makeCsv( inputs='source/maps.csv' )

#
# Layer summary
#

layerSummary = db.makeQuery( 
  name='layerSummary', 
  inputs=[ tblConfigMapLayers, dataSources ],
  sql="""
SELECT 
  lyr.Overlay_Name             AS overlayName,
  lyr.Oracle_View_Name         AS name, 
  lyr.[Feature/Interest_Type]  AS description,
  ds.dataSource                AS dataSourceId,
  lyr.SOS_Report               AS sos,
  lyr.Map_Viewer               AS map
FROM tblConfigMapLayers lyr
  JOIN dataSources ds ON lyr.queried_from = ds.queried_from
WHERE lyr.deleted_ind = 0 
  AND lyr.v2_view_ready = 1
  AND lyr.deferred_from_v2 = 0
ORDER BY overlayName, name 
""" )

#
# SOS Layers
#

sosLayers = db.makeQuery( 
  name='sosLayers', 
  inputs=[ tblConfigMapLayers, dataSources, formats ],
  sql="""
SELECT 
  lyr.Oracle_View_Name         AS name, 
  lyr.[Feature/Interest_Type]  AS description,
  ds.dataSource                AS dataSourceId,
  CASE lyr.Internal_Only WHEN 1 THEN "AS_REPORT_"||lyr.Oracle_View_Name ELSE "AS_REPORT_SCOPE" END AS oauth2ReportScopeName,
  fmt.title_format,
  fmt.summary_format
FROM tblConfigMapLayers lyr
  JOIN dataSources ds ON lyr.queried_from = ds.queried_from
  LEFT JOIN formats fmt ON fmt.Oracle_View_Name = lyr.Oracle_View_Name
WHERE lyr.deleted_ind = 0 
  AND lyr.v2_view_ready = 1
  AND lyr.deferred_from_v2 = 0
  AND lyr.SOS_Report = 1
ORDER BY name 
""" )

#
# feature attributes
#

featureAttributes = db.makeQuery( 
  name='featureAttributes', 
  inputs=[ sosLayers, tblConfigMapAttributes, columnTypes ],
  sql="""
SELECT 
  lyr.name                         AS featureName, 
  attr.OSDB_COLUMN_NAME            AS name,
  attr.TITLE                       AS title,
  attr.COLUMN_ORDER                AS [order],
  attr.TITLE                       AS description,
  IFNULL( ct.ATTR_TYPE, "String" ) AS attributeTypeCode
FROM sosLayers lyr 
  JOIN tblConfigMapAttributes attr ON lyr.name = attr.TABLE_NAME
  LEFT JOIN columnTypes ct ON attr.TABLE_NAME = ct.TABLE_NAME AND attr.OSDB_COLUMN_NAME = ct.COLUMN_NAME
WHERE ( attr.INCLUDE_IN_SOS_PUBLIC = 1 OR attr.INCLUDE_IN_IDENTIFY_PUBLIC = 1 )
ORDER BY featureName, name 
""" )

#
# add 'All' layer category
#

layerCategoryAll = db.makeQuery( 
  name='layerCategoryAll', 
  inputs=[ layerCategories ],
  sql="""
SELECT
  'All'              AS category,
  name               AS name,
  MAX( include )     AS include,
  MAX( significant ) AS significant,
  MAX( display )     AS display
FROM layerCategories lc
GROUP BY name
ORDER BY name
""" )

layerCategoriesUnion = db.makeQuery( 
  name='layerCategoriesUnion', 
  inputs=[ layerCategoryAll, layerCategories ],
  sql="""
SELECT * FROM layerCategories 
UNION ALL
SELECT * FROM layerCategoryAll 
ORDER BY category, name
""" )

#
# query template layers
#

queryTemplateLayers = db.makeQuery( 
  name='queryTemplateLayers', 
  inputs=[ layerCategoriesUnion, sosLayers ],
  sql="""
SELECT
  lc.category            AS templateName,
  lyr.name               AS name,
  lyr.description        AS description,
  lyr.name               AS featureName,
  lyr.title_format       AS titleFormat,
  lyr.summary_format     AS summaryFormat
FROM layerCategoriesUnion lc
  JOIN sosLayers lyr ON lc.name = lyr.name
WHERE lc.include = 1
ORDER BY templateName, lc.significant desc, description
""" )

#
# query template layer attributes
#

queryTemplateLayerAttributes = db.makeQuery( 
  name='queryTemplateLayerAttributes', 
  inputs=[ layerCategoriesUnion, sosLayers, tblConfigMapAttributes ],
  sql="""
SELECT
  lc.category                        AS templateName,
  attr.TABLE_NAME                    AS templateLayerName,
  attr.OSDB_COLUMN_NAME              AS name,
  attr.TITLE                         AS title,
  CAST(attr.COLUMN_ORDER AS INTEGER) AS [order],
  attr.TITLE                         AS description,
  attr.OSDB_COLUMN_NAME              AS featureAttributeName,
  attr.BUSINESS_KEY                  AS businessKey
FROM layerCategoriesUnion lc
  JOIN sosLayers lyr ON lc.name = lyr.name
  JOIN tblConfigMapAttributes attr ON lyr.name = attr.TABLE_NAME
WHERE lc.include = 1
  AND attr.INCLUDE_IN_SOS_PUBLIC = 1
""" )

#
# overlays
# 

overlays = db.makeQuery( 
  name='overlays', 
  inputs=[ tblConfigMapLayers ],
  sql="""
SELECT
  lyr.Overlay_Name                                          AS title,
  UPPER(REPLACE(REPLACE(lyr.Overlay_Name,',',''),' ','_'))  AS id
FROM tblConfigMapLayers lyr
GROUP BY lyr.Overlay_Name
""" )

#
# Map Layers
#

mapLayers = db.makeQuery( 
  name='mapLayers', 
  inputs=[ tblConfigMapLayers, dataSources, overlays ],
  sql="""
SELECT 
  lyr.Oracle_View_Name         AS name, 
  lyr.[Feature/Interest_Type]  AS description,
  lyr.Publicly_Downloadable    AS identifiable,
  ds.dataStore                 AS dataStore,
  ov.id                        as overlayId
FROM tblConfigMapLayers lyr
  JOIN dataSources ds ON lyr.queried_from = ds.queried_from
  JOIN overlays ov ON lyr.Overlay_Name = ov.title
WHERE lyr.deleted_ind = 0 
  AND lyr.v2_view_ready = 1
  AND lyr.deferred_from_v2 = 0
  AND lyr.Map_Viewer = 1
ORDER BY name 
""" )

#
# map overlays
# 

# mapName, title, id, -- layerType, url, transparent, singleTile, opacity, expanded, visibility

mapOverlays = db.makeQuery( 
  name='mapOverlays', 
  inputs=[ layerCategoriesUnion, overlays, mapLayers, overlayUrls ],
  sql="""
SELECT
  lc.category  AS mapName,
  ov.title     AS title,
  ov.id        AS id,
  IFNULL( ou.url, 'URL_DEFAULT' ) AS url
FROM mapLayers lyr
  JOIN overlays ov ON lyr.overlayId = ov.id
  JOIN layerCategoriesUnion lc ON lyr.name = lc.name
  LEFT JOIN overlayUrls ou ON ov.id = ou.id
GROUP BY title, mapName
""" )

#
# map overlay layers
# 

# mapName, overlayId, title, id, layers, styles, identifiable, selectable, visibility -- max_scale, url, geometryAttribute, titleAttribute

mapOverlayLayers = db.makeQuery( 
  name='mapOverlayLayers', 
  inputs=[ layerCategoriesUnion, mapLayers, overlayLayers ],
  sql="""
SELECT
  lc.category                   AS mapName,
  lyr.overlayId                 AS overlayId,
  lc.significant                AS sig, 
  lyr.description               AS title,
  lyr.name                      AS id,
  IFNULL( ol.layers, lyr.name ) AS layers,
  IFNULL( ol.styles, lyr.name ) AS styles,
  lyr.identifiable              AS identifiable,
  0                             AS selectable,
  lc.display                    AS visibility,
  IFNULL(ol.max_scale,0)        AS max_scale,
  ol.geometryAttribute          AS geometryAttribute
FROM layerCategoriesUnion lc
 JOIN mapLayers lyr ON lyr.name = lc.name
 LEFT JOIN overlayLayers ol ON ol.name = lc.name
WHERE lc.include = 1
ORDER BY mapName, overlayId, lc.significant desc, title
""" )

#
# map overlay layer attributes
# 

# mapName, overlayId, layerId, name, title

mapOverlayLayerAttributes = db.makeQuery( 
  name='mapOverlayLayerAttributes', 
  inputs=[ layerCategoriesUnion, mapLayers, tblConfigMapAttributes ],
  sql="""
SELECT
  lc.category                 AS mapName,
  lyr.overlayId               AS overlayId,
  lyr.name                    AS layerId,
  attr.OSDB_COLUMN_NAME       AS name,
  attr.TITLE                  AS title
FROM layerCategoriesUnion lc
  JOIN mapLayers lyr ON lyr.name = lc.name
  JOIN tblConfigMapAttributes attr ON lyr.name = attr.TABLE_NAME
WHERE lc.include = 1
  AND attr.INCLUDE_IN_IDENTIFY_PUBLIC = 1
ORDER BY mapName, overlayId, layerId, name
""" )

#
# 
#

if __name__ == "__main__":
  layerSummary.writeCsv()

  sosLayers.writeCsv( 'features.csv' )

  featureAttributes.writeCsv( 'feature-attributes.csv' )

  queryTemplateLayers.writeCsv( 'query-template-layers.csv' )

  queryTemplateLayerAttributes.writeCsv( 'query-template-layer-attributes.csv' )

  mapLayers.writeCsv( 'map-layers.csv' )

  mapOverlays.writeCsv( 'map-overlays.csv' )

  mapOverlayLayers.writeCsv( 'map-overlay-layers.csv' )

  mapOverlayLayerAttributes.writeCsv( 'map-overlay-layer-attributes.csv' )

#
# map overlays unioning all categories
# 

# csvsql --query "
# SELECT
#     'All'                       AS mapName,
#     overlayId                   AS overlayId,
#     max(sig)                    AS sig, 
#     title                       AS title,
#     id                          AS id,
#     layers                      AS layers,
#     styles                      AS styles,
#     max(identifiable)           AS identifiable,
#     0                           AS selectable,
#     max(visibility)             AS visibility
# FROM [map-overlay-layers] lyr
# GROUP BY id
# ORDER BY mapName, overlayId, sig desc, title
# " ../tables/map-overlay-layers.csv | csvformat -U 2 > ../tables/all-overlay-layers.csv

# ls -l ../tables/all-overlay-layers.csv

# csvsql --query "
# SELECT
#     'All'                       AS mapName,
#     overlayId                   AS overlayId,
#     layerId                     AS layerId,
#     name                        AS name,
#     title                       AS title
# FROM [map-overlay-layer-attributes] attr
# GROUP BY layerId, name
# ORDER BY mapName, overlayId, layerId, name
# " ../tables/map-overlay-layer-attributes.csv | csvformat -U 2 > ../tables/all-overlay-layer-attributes.csv

# ls -l ../tables/all-overlay-layer-attributes.csv


#
# overlay titles
# 

# csvsql  --query '
# SELECT 
#     replace(replace(lyr.Overlay_name," ",""),",","") AS name, 
#     lyr.Overlay_name                                 AS title 
# FROM tblConfigMapLayers lyr
# WHERE title <> ""  
# group by Overlay_name
# ' tblConfigMapLayers.csv | csvformat -U 2 > tmp/overlays.csv

#
# overlays
#

# for qt in ${CATEGORIES[*]}
# do
#     csvsql --query "
#     SELECT 
#         '$qt' AS mapId, 
#         o.name, 
#         o.title 
#     FROM overlays o
#     " tmp/overlays.csv | csvformat -U 2 > tmp/$qt-overlays.csv
# done

# csvstack tmp/*-overlays.csv | csvformat -U 2 > ../tables/overlays.csv

# ls -l ../tables/overlays.csv
