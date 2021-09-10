def read_tiles(tasks, mask):
    from osgeo import gdal,ogr,osr

    tasks_ds = ogr.Open(tasks, 0)
    tasks_lyr = tasks_ds.GetLayer(0)

    '''打开mask shp并获取几何'''
    ds_mask = ogr.Open(mask, 0)
    lyr_mask = ds_mask.GetLayer(0)
    mask_feature = lyr_mask.GetNextFeature()
    mask_geometry = mask_feature.geometry().Clone()

    tiles = []

    tasks_lyr.SetSpatialFilter(mask_geometry)# 首先进行空间筛选

    for tile_feat in tasks_lyr:
        tiles.append(tile_feat.GetField('tag'))
        
    del tasks_ds

    return tiles

def mosaic_clip(tifs, mask, year, pathout):
    from osgeo import gdal,gdalconst,ogr,osr
    import os

    ds_tif = gdal.Open(tifs[0], gdalconst.GA_ReadOnly) # 输入文件
    band_tif = ds_tif.GetRasterBand(1)
    nodata = band_tif.GetNoDataValue()
    proj_tif = ds_tif.GetProjection()
    
    ds_mosaic = gdal.Warp('', tifs, format='MEM', srcSRS=proj_tif, dstSRS=proj_tif, srcNodata=nodata, dstNodata=nodata)

    ds_clip = gdal.Warp('', ds_mosaic, format='MEM', cutlineDSName=mask, cropToCutline=True, srcNodata=nodata, dstNodata=nodata)

    out_data = pathout + '/' + 'tcc_' + str(year) + '.tif'

    driver = gdal.GetDriverByName('GTiff')
    driver.CreateCopy(out_data, ds_clip, strict=1, options=["TILED=YES", "COMPRESS=LZW"])

    del ds_tif
    del ds_mosaic
    del ds_clip
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

    from osgeo import gdal,ogr,osr
    import os
    import random

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', type=str, help='input', required=True)# 输入路径
    parser.add_argument('-o', '--output', type=str, help='output', required=True)# 输出路径
    parser.add_argument('-ta', '--tasks', type=str, help='tasks', required=True)# task_shp
    parser.add_argument('-m', '--mask', type=str, help='mask', required=True)# mask
    args = parser.parse_args()

    if comm_rank == 0:
        tiles = read_tiles(args.tasks, args.mask)
        datas = [x+1984 for x in range(36)]
        random.shuffle(datas)
        mpi_datas = divide(datas, comm_size)
    else:
        tiles = None
        datas = None
        mpi_datas = None

    mpi_data_divide = comm.scatter(mpi_datas, root=0)
    tiles = comm.bcast(tiles, root=0)

    for year in mpi_data_divide:
        tile_datas = []
        for tile in tiles:
            tile_data = args.input + '/' + tile[0:4] + '/' + tile[-4:] + '/' + tile + '/' + tile + '_y' + str(year) + '_dat.tif'
            tile_datas.append(tile_data)

        mosaic_clip(tile_datas, args.mask, year, args.output)
        
    return

if __name__ == "__main__":
    main()

    

    





