def read_tiles(tasks):
    from osgeo import gdal,ogr,osr

    tasks_ds = ogr.Open(tasks, 0)
    tasks_lyr = tasks_ds.GetLayer(0)

    tiles = []

    for tile_feat in tasks_lyr:
        tiles.append(tile_feat.GetField('tag'))
        
    del tasks_ds

    return tiles


def retiles(tasks_albers, tasks_wgs84, tile, year, pathin, pathout):
    from osgeo import gdal,gdalconst,ogr,osr

    import os
    os.environ['PROJ_LIB'] = r'/public/home/mfeng/anaconda3/envs/geo/share/proj'

    '''获取tile左上角坐标，设置GeoTransform，获取投影信息'''
    ds_albers_tasks = ogr.Open(tasks_albers, 0)
    lyr_albers_tasks = ds_albers_tasks.GetLayer(0)

    spatialRef_albers_tasks = lyr_albers_tasks.GetSpatialRef()
    spatialRef_albers_tasks_wkt = spatialRef_albers_tasks.ExportToWkt()# 投影信息

    expression = "{} = '{}'".format('tag', tile)

    lyr_albers_tasks.SetAttributeFilter(expression)

    feature_albers_tile = lyr_albers_tasks.GetNextFeature()
    geometry_albers_tile = feature_albers_tile.geometry().Clone()

    ring = geometry_albers_tile.GetGeometryRef(0)
    point_1 = ring.GetPoints()[0]# tile左上角坐标
    point_3 = ring.GetPoints()[2]# tile右下角坐标

    '''打开tasks_wgs84，并进行空间筛选，判断与geometry_albers_tile相交的tiles并存入列表'''
    ds_wgs84_tasks = ogr.Open(tasks_wgs84, 0)
    lyr_wgs84_tasks = ds_wgs84_tasks.GetLayer(0)

    lyr_wgs84_tasks.SetSpatialFilter(geometry_albers_tile)# 空间筛选

    tifs = []

    for feat in lyr_wgs84_tasks:
        tif = feat.GetField('tag')
        tif_data = pathin + '/' + tif[0:4] + '/' + tif[-4:] + '/' + tif + '/' + tif + '_y' + year + '_dat.tif'

        if os.path.exists(tif_data):
            tifs.append(tif_data)
        else:
            print(tif, ' not exist')

    if len(tifs) == 1:
        ds_tif = gdal.Open(tifs[0], gdalconst.GA_ReadOnly) # 输入文件
        band_tif = ds_tif.GetRasterBand(1)
        nodata = band_tif.GetNoDataValue()
        proj_tif = ds_tif.GetProjection()

        ds_proj = gdal.Warp('', tifs[0], format='MEM', xRes=30, yRes=30, srcSRS=proj_tif, dstSRS=spatialRef_albers_tasks_wkt, srcNodata=nodata, dstNodata=nodata)

        pathout_1 = pathout + '/' + tile[0:4] + '/' + tile[-4:] + '/' + tile

        if os.path.isdir(pathout_1):
            pass
        else:
            try:
                os.makedirs(pathout_1)
            except:
                pass

        ds_clip = gdal.Warp('', ds_proj, format='MEM', outputBounds=(point_1[0],point_3[1],point_3[0],point_1[1]), outputBoundsSRS=spatialRef_albers_tasks_wkt, xRes=30, yRes=30, srcSRS=spatialRef_albers_tasks_wkt, dstSRS=spatialRef_albers_tasks_wkt, srcNodata=nodata, dstNodata=nodata)
        
        out_data = pathout_1 + '/' + tile + '_y' + year + '_dat.tif'

        driver = gdal.GetDriverByName('GTiff')
        driver.CreateCopy(out_data, ds_clip, strict=1, options=["TILED=YES", "COMPRESS=LZW"])
        
        del ds_proj
        del ds_clip

    elif len(tifs) > 1:
        ds_tif = gdal.Open(tifs[0], gdalconst.GA_ReadOnly) # 输入文件
        band_tif = ds_tif.GetRasterBand(1)
        nodata = band_tif.GetNoDataValue()
        proj_tif = ds_tif.GetProjection()
        
        ds_mosaic = gdal.Warp('', tifs, format='MEM', srcNodata=nodata, dstNodata=nodata)

        ds_proj = gdal.Warp('', ds_mosaic, format='MEM', xRes=30, yRes=30, srcSRS=proj_tif, dstSRS=spatialRef_albers_tasks_wkt, srcNodata=nodata, dstNodata=nodata)

        pathout_1 = pathout + '/' + tile[0:4] + '/' + tile[-4:] + '/' + tile

        if os.path.isdir(pathout_1):
            pass
        else:
            try:
                os.makedirs(pathout_1)
            except:
                pass

        ds_clip = gdal.Warp('', ds_proj, format='MEM', outputBounds=(point_1[0],point_3[1],point_3[0],point_1[1]), outputBoundsSRS=spatialRef_albers_tasks_wkt, xRes=30, yRes=30, srcSRS=spatialRef_albers_tasks_wkt, dstSRS=spatialRef_albers_tasks_wkt, srcNodata=nodata, dstNodata=nodata)
        
        out_data = pathout_1 + '/' + tile + '_y' + year + '_dat.tif'

        driver = gdal.GetDriverByName('GTiff')
        driver.CreateCopy(out_data, ds_clip, strict=1, options=["TILED=YES", "COMPRESS=LZW"])
        
        del ds_mosaic
        del ds_proj
        del ds_clip

    else:
        print(tile, ' no intersect')

    del ds_albers_tasks
    del ds_wgs84_tasks
    del ds_tif

    return


def divide(datas, n):
    '''进程分割'''

    mpi_datas = {}
    step = len(datas)//n
    for i in range(n):
        if i < n-1:
            mpi_data = datas[i*step:(i+1)*step]
            mpi_datas[i] = mpi_data
        else:
            mpi_data = datas[i*step:]
            mpi_datas[i] = mpi_data

    j = 0
    while len(mpi_datas[n-1]) > step and j < n-1:
        mpi_datas[j].append(mpi_datas[n-1][-1])
        mpi_datas[n-1].remove(mpi_datas[n-1][-1])
        j = j + 1
    
    mpi_datas_out = []
    for mpi_data_out in mpi_datas.values():
        mpi_datas_out.append(mpi_data_out)
    return mpi_datas_out


def main():
    import mpi4py.MPI as MPI
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()

    import random
    import os

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-ta', '--tasks_albers', type=str, help='tasks_albers', required=True)# tasks_albers
    parser.add_argument('-tw', '--tasks_wgs84', type=str, help='tasks_wgs84', required=True)# tasks_wgs84
    parser.add_argument('-i', '--input', type=str, help='input', required=True)# 输入路径
    parser.add_argument('-o', '--output', type=str, help='output', required=True)# 输出路径
    args = parser.parse_args()

    if comm_rank == 0:
        years = [x+1984 for x in range(36)]
        random.shuffle(years)
        mpi_years = divide(years, comm_size)

        tiles = read_tiles(args.tasks_albers)
    else:
        years = None
        mpi_years = None
        tiles = None

    mpi_years_divide = comm.scatter(mpi_years, root=0)
    tiles = comm.bcast(tiles, root=0)

    if os.path.isdir(args.output):
        pass
    else:
        try:
            os.makedirs(args.output)
        except:
            pass

    for year in mpi_years_divide:
        for tile in tiles:

            retiles(args.tasks_albers, args.tasks_wgs84, tile, str(year), args.input, args.output)


    return

if __name__ == '__main__':
    main()

