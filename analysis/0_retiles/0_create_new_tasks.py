def create_albers_tiles(mask, resolution, number_of_pixels, extend, pathout_albers_tasks):
    from osgeo import ogr, osr

    ds_mask = ogr.Open(mask, 0)
    lyr_mask = ds_mask.GetLayer(0)
    extent = lyr_mask.GetExtent()
    mask_feature = lyr_mask.GetNextFeature()
    mask_geometry = mask_feature.geometry().Clone()

    W = extent[0]
    E = extent[1]
    S = extent[2]
    N = extent[3]

    driver_temp = ogr.GetDriverByName('Memory')
    ds_temp = driver_temp.CreateDataSource('temp')
    lyr_temp = ds_temp.CreateLayer('out', lyr_mask.GetSpatialRef(), ogr.wkbPolygon)

    # 添加frequency字段
    fld = ogr.FieldDefn('tag', ogr.OFTString)
    fld.SetWidth(20)
    lyr_temp.CreateField(fld)
    
    h = 0
    v = 0

    start_lon = W
    while start_lon <= E:
        end_lon = start_lon + (resolution * (number_of_pixels + extend))

        start_lat = N
        v = 0
        while start_lat >= S:
            end_lat = start_lat - (resolution * (number_of_pixels + extend))

            # 创建几何
            feat = ogr.Feature(lyr_temp.GetLayerDefn())
            ring = ogr.Geometry(ogr.wkbLinearRing)
            ring.AddPoint(start_lon, start_lat)# 左上角
            ring.AddPoint(end_lon, start_lat)# 右上角
            ring.AddPoint(end_lon, end_lat)# 右下角
            ring.AddPoint(start_lon, end_lat)# 左下角
            ring.AddPoint(start_lon, start_lat)# 左上角
            yard = ogr.Geometry(ogr.wkbPolygon)
            yard.AddGeometry(ring)
            feat.SetGeometry(yard)

            # 写入属性
            feat.SetField('tag', 'h'+str(h).zfill(3)+'v'+str(v).zfill(3))
            lyr_temp.CreateFeature(feat)

            start_lat = start_lat - (resolution * number_of_pixels)
            v = v + 1

        start_lon = start_lon + (resolution * number_of_pixels)
        h = h + 1

    lyr_temp.SetSpatialFilter(mask_geometry)

    driver_tasks = ogr.GetDriverByName('ESRI Shapefile')
    ds_tasks = driver_tasks.CreateDataSource(pathout_albers_tasks)
    lyr_tasks = ds_tasks.CreateLayer('tasks', lyr_mask.GetSpatialRef(), ogr.wkbPolygon)

    lyr_tasks.CreateFields(lyr_temp.schema)

    out_feat = ogr.Feature(lyr_tasks.GetLayerDefn())

    for in_feat in lyr_temp:
        geom = in_feat.geometry()
        out_feat.SetGeometry(geom)
        for i in range(in_feat.GetFieldCount()):
            value = in_feat.GetField(i)
            out_feat.SetField(i, value)
        lyr_tasks.CreateFeature(out_feat)

    del ds_mask
    del ds_temp
    del ds_tasks

    return

def main():
    mask = r'/public/home/mfeng/jwang/paper/10_ne/data/shp/NE_WGS1984.shp'
    resolution = 0.00025
    number_of_pixels = 1000
    extend = 0
    pathout_albers_tasks = r'/public/home/mfeng/jwang/paper/10_ne/data/tasks/tasks_wgs.shp'

    create_albers_tiles(mask, resolution, number_of_pixels, extend, pathout_albers_tasks)
    return

if __name__ == '__main__':
    main()

# python create_albers_tiles.py

