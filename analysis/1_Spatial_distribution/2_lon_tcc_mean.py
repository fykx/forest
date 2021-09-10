'''
Descripttion: 
version: 0.1
Author: Jianbang Wang
Date: 2021-05-13
'''

def contains_or_intersects(tasks, mask):
    '''
    判断tiles与mask包含还是相交，并分别生成数据列表
    '''

    from osgeo import gdal,ogr,osr

    '''打开mask shp并获取几何'''
    ds_mask = ogr.Open(mask, 0)
    lyr_mask = ds_mask.GetLayer(0)
    mask_feature = lyr_mask.GetNextFeature()
    mask_geometry = mask_feature.geometry().Clone()

    tasks_ds = ogr.Open(tasks, 0)
    tasks_lyr = tasks_ds.GetLayer(0)

    contains_tiles = []# 包含的瓦片，不需要裁剪
    intersects_tiles = []# 相交的瓦片，需要裁剪

    tasks_lyr.SetSpatialFilter(mask_geometry)# 首先进行空间筛选

    '''将与mask相交、包含的tile分别存入列表'''
    for tile_feat in tasks_lyr:
        tile_geometry = tile_feat.geometry()
        if mask_geometry.Intersects(tile_geometry) == True:
            if mask_geometry.Contains(tile_geometry) == True:
                contains_tiles.append(tile_feat.GetField('tag'))
            else:
                intersects_tiles.append(tile_feat.GetField('tag'))
        else:
            pass
        
    del ds_mask
    del tasks_ds

    return contains_tiles, intersects_tiles


def stat(tile, pathin_tcc, pathin_para, mask, contains_tiles, intersects_tiles, pathout):
    from osgeo import gdal,ogr,osr
    import numpy as np
    import pandas as pd
    import os

    tcc = pathin_tcc + '/' + tile + '_mean.tif'
    para = pathin_para + '/' + tile + '_lon.tif'

    if tile in contains_tiles:

        ds_tcc = gdal.Open(tcc)
        in_band_tcc = ds_tcc.GetRasterBand(1)# 波段索引从1开始
        tcc_array = in_band_tcc.ReadAsArray()

        ds_para = gdal.Open(para)
        in_band_para = ds_para.GetRasterBand(1)# 波段索引从1开始
        para_array = in_band_para.ReadAsArray()

        tcc_array_0 = np.ma.masked_where(tcc_array == -9999, tcc_array)# 掩膜去除影像nodata
        para_array_0 = np.ma.masked_where(para_array == -9999, para_array)# 掩膜去除影像nodata

        boundary_step = [x*0.1+111.5 for x in range(240)]# 设置统计范围及步长

        df_out = pd.DataFrame(columns=['lon', 'mean'])

        for i in range(len(boundary_step)-1):

            value_dict = {}

            start = boundary_step[i]
            end = boundary_step[i+1]

            tcc_mask_array_0 = np.ma.masked_where(para_array_0 < start, tcc_array_0)# 设定统计区间
            tcc_mask_array_1 = np.ma.masked_where(para_array_0 >= end, tcc_mask_array_0)# 设定统计区间

            if tcc_mask_array_1.count() > 0:
                value_mean = tcc_mask_array_1.mean()# 计算均值
            else:
                value_mean = -9999

            value_dict['lon'] = [start]
            value_dict['mean'] = [value_mean]
            df_out = df_out.append(pd.DataFrame(value_dict))

        if os.path.isdir(pathout):
            pass
        else:
            try:
                os.makedirs(pathout)
            except:
                pass

        df_out.to_csv(pathout + '/' + tile + '.csv', index=False, header=True)

        del ds_tcc
        del ds_para


    if tile in intersects_tiles:
        ds_tcc = gdal.Warp('', tcc, format='MEM', cutlineDSName=mask, dstNodata=-9999)# 使用mask裁剪tile，并保存至虚拟栅格
        in_band_tcc = ds_tcc.GetRasterBand(1)# 波段索引从1开始
        tcc_array = in_band_tcc.ReadAsArray()

        ds_para = gdal.Warp('', para, format='MEM', cutlineDSName=mask, dstNodata=-9999)# 使用mask裁剪tile，并保存至虚拟栅格
        in_band_para = ds_para.GetRasterBand(1)# 波段索引从1开始
        para_array = in_band_para.ReadAsArray()

        tcc_array_0 = np.ma.masked_where(tcc_array == -9999, tcc_array)# 掩膜去除影像nodata
        para_array_0 = np.ma.masked_where(para_array == -9999, para_array)# 掩膜去除影像nodata

        boundary_step = [x*0.1+111.5 for x in range(240)]# 设置统计范围及步长

        df_out = pd.DataFrame(columns=['lon', 'mean'])

        for i in range(len(boundary_step)-1):

            value_dict = {}

            start = boundary_step[i]
            end = boundary_step[i+1]

            tcc_mask_array_0 = np.ma.masked_where(para_array_0 < start, tcc_array_0)# 设定统计区间
            tcc_mask_array_1 = np.ma.masked_where(para_array_0 >= end, tcc_mask_array_0)# 设定统计区间

            if tcc_mask_array_1.count() > 0:
                value_mean = tcc_mask_array_1.mean()# 计算均值
            else:
                value_mean = -9999

            value_dict['lon'] = [start]
            value_dict['mean'] = [value_mean]
            df_out = df_out.append(pd.DataFrame(value_dict))

        if os.path.isdir(pathout):
            pass
        else:
            try:
                os.makedirs(pathout)
            except:
                pass

        df_out.to_csv(pathout + '/' + tile + '.csv', index=False, header=True)

        del ds_tcc
        del ds_para

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
    import pandas as pd
    import os
    import random

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-ta', '--tasks', type=str, help='tasks', required=True)# task_shp
    parser.add_argument('-i_tcc', '--input_tcc', type=str, help='input_tcc', required=True)# 输入路径
    parser.add_argument('-i_para', '--input_para', type=str, help='input_para', required=True)# 输入路径
    parser.add_argument('-mask', '--mask', type=str, help='mask', required=True)# 输入路径
    parser.add_argument('-o', '--output', type=str, help='output', required=True)# 输出路径

    args = parser.parse_args()

    if comm_rank == 0:
        contains_tiles, intersects_tiles = contains_or_intersects(args.tasks, args.mask)
        datas = contains_tiles + intersects_tiles
        random.shuffle(datas)
        mpi_datas = divide(datas, comm_size)
    else:
        contains_tiles = None
        intersects_tiles = None
        datas = None
        mpi_datas = None

    mpi_data_divide = comm.scatter(mpi_datas, root=0)
    contains_tiles = comm.bcast(contains_tiles, root=0)
    intersects_tiles = comm.bcast(intersects_tiles, root=0)

    for tile in mpi_data_divide:
        stat(tile, args.input_tcc, args.input_para, args.mask, contains_tiles, intersects_tiles, args.output)

    return


if __name__ == '__main__':
    main()