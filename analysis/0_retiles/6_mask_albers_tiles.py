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
    

def generate_mask(tile, pathin, pathout):
    from osgeo import gdal,gdalconst,ogr,osr
    import numpy as np

    pathin_tile = pathin + '/' + tile[0:4] + '/' + tile[-4:] + '/' + tile

    datas = listdatas(pathin_tile)

    in_ds_para = gdal.Open(datas[0])
    in_band_para = in_ds_para.GetRasterBand(1)# 波段索引从1开始
    in_array_para = in_band_para.ReadAsArray()
    xsize_para = in_band_para.XSize# 列
    ysize_para = in_band_para.YSize# 行
    nodata_para = in_band_para.GetNoDataValue()

    # 新建数据集
    gtiff_driver = gdal.GetDriverByName('GTiff')
    out_ds = gtiff_driver.Create(pathout + '/' + tile + '_mask.tif', xsize_para, ysize_para, 1, in_band_para.DataType)
    out_ds.SetProjection(in_ds_para.GetProjection())
    out_ds.SetGeoTransform(in_ds_para.GetGeoTransform())

    del in_ds_para

    datas_list = []

    for data in datas:
        in_ds = gdal.Open(data)
        in_band = in_ds.GetRasterBand(1)# 波段索引从1开始
        in_array = in_band.ReadAsArray()

        datas_list.append(in_array)

        del in_ds

    datas_narray = np.array(datas_list)

    #构建输出数组
    mask = np.zeros(shape=(ysize_para, xsize_para))

    for x in range(xsize_para):# 遍历列
        for y in range(ysize_para):# 遍历行

            value = 0

            threshold_nodata = datas_narray[:,y,x].tolist().count(nodata_para)

            if threshold_nodata > 5:
                value = 1

            mask[y, x] = value


    out_band = out_ds.GetRasterBand(1)
    out_band.FlushCache()
    out_band.WriteArray(mask)
    out_band.SetNoDataValue(nodata_para)
    out_band.ComputeStatistics(False)

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

    for data in mpi_datas_divide:

        generate_mask(data, args.input, args.output)

    return

if __name__ == "__main__":
    main()