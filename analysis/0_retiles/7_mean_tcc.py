def read_tiles(tasks):
    from osgeo import gdal,ogr,osr

    tasks_ds = ogr.Open(tasks, 0)
    tasks_lyr = tasks_ds.GetLayer(0)

    tiles = []

    for tile_feat in tasks_lyr:
        tiles.append(tile_feat.GetField('tag'))
        
    del tasks_ds

    return tiles


def listdatas(pathin):
    import os

    a = []
    datas = os.listdir(pathin)
    for i in datas:
        if i[-4:] == '.tif':
            fn_i = pathin + '/' + i
            a.append(fn_i)
    return a


def mean_datas(tile, pathin, pathout):
    from osgeo import gdal
    import numpy as np
    import math

    datas = listdatas(pathin + '/' + tile[0:4] + '/' + tile[-4:] + '/' + tile)

    three_d_list = []

    for data in datas:
        in_ds = gdal.Open(data)
        in_band = in_ds.GetRasterBand(1)# 波段索引从1开始
        in_array = in_band.ReadAsArray()
        three_d_list.append(in_array)

        del in_ds

    three_d_array = np.array(three_d_list)

    # 打开para
    in_ds_para = gdal.Open(datas[0])
    in_band_para = in_ds_para.GetRasterBand(1)# 波段索引从1开始
    in_array_para = in_band_para.ReadAsArray()
    xsize_para = in_band_para.XSize# 列
    ysize_para = in_band_para.YSize# 行
    nodata_para = in_band_para.GetNoDataValue()

    out_data = np.zeros(shape=(ysize_para, xsize_para))

    for x in range(xsize_para):# 遍历列
        for y in range(ysize_para):# 遍历行
            
            value_array = three_d_array[:,y,x]
            value_array_masked = np.ma.masked_where(value_array>100, value_array)

            if value_array_masked.count() > 0:
                mean_value = value_array_masked.mean()
                out_data[y,x] = mean_value
            else:
                out_data[y,x] = -9999
    

    # 新建数据集
    gtiff_driver = gdal.GetDriverByName('GTiff')
    out_ds = gtiff_driver.Create(pathout + '/' + tile + '_mean.tif', xsize_para, ysize_para, 1, gdal.GDT_Float32)# in_band_para.DataType
    out_ds.SetProjection(in_ds_para.GetProjection())
    out_ds.SetGeoTransform(in_ds_para.GetGeoTransform())

    #数据输出            
    out_band = out_ds.GetRasterBand(1)
    out_band.FlushCache()
    out_band.WriteArray(out_data)
    out_band.SetNoDataValue(-9999)#设置nodata
    out_band.ComputeStatistics(False)

    del in_ds_para

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
    parser.add_argument('-i', '--input', type=str, help='input', required=True)# 输入路径
    parser.add_argument('-o', '--output', type=str, help='output', required=True)# 输出路径
    args = parser.parse_args()

    if comm_rank == 0:
        tiles = read_tiles(args.tasks_albers)
        random.shuffle(tiles)
        mpi_datas = divide(tiles, comm_size)
    else:
        tiles = None
        mpi_datas = None

    mpi_datas_divide = comm.scatter(mpi_datas, root=0)

    if os.path.isdir(args.output):
        pass
    else:
        try:
            os.makedirs(args.output)
        except:
            pass

    for tile in mpi_datas_divide:

        mean_datas(tile, args.input, args.output)

    return


if __name__ == '__main__':
    main()