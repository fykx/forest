def listdatas(pathin):
    import os
    a = []
    datas = os.listdir(pathin)
    for i in datas:
        if i.endswith('.csv'):
            fn_i = pathin + '/' + i
            a.append(fn_i)
    return a


def extract(lon, lat, tasks, pathin):
    from osgeo import ogr,gdal

    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(lon, lat)
    
    ds_tasks = ogr.Open(tasks,1)
    lyr_tasks = ds_tasks.GetLayer(0)

    lyr_tasks.SetSpatialFilter(point)

    tiles = []
    for feat_task in lyr_tasks:
        tag_tile = feat_task.GetField('tag')
        tiles.append(tag_tile)

    if len(tiles) != 0:
        for tile in tiles:
            tif_tile = pathin + '/' + tile[0:5] + '/' + tile[-4:] + '/' + tile + '/' + tile + '_y2019_dat.tif'

            in_ds = gdal.Open(tif_tile)
            in_band = in_ds.GetRasterBand(1)# 波段索引从1开始
            xsize = in_band.XSize# 列
            ysize = in_band.YSize# 行
            nodata = in_band.GetNoDataValue()

            in_data = in_band.ReadAsArray()

            transform = in_ds.GetGeoTransform()
            xOrigin = transform[0]# 左上角位置
            yOrigin = transform[3]# 左上角位置
            pixelWidth = transform[1]# 像元宽度
            pixelHeight = transform[5]# 像元高度

            xOffset = int((lon-xOrigin)/pixelWidth)
            yOffset = int((lat-yOrigin)/pixelHeight)

            if xOffset < xsize and yOffset < ysize:
                value = in_data[yOffset,xOffset]
                break
            else:
                value = -9999 #未正确提取
                continue

            del in_ds

    else:
        value = -99 #不在研究区内

    del ds_tasks

    return value


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
    parser.add_argument('-i_tcc', '--input_tcc', type=str, help='input_tcc', required=True)# 输入路径
    parser.add_argument('-i_csv', '--input_csv', type=str, help='input_csv', required=True)# 输入路径
    parser.add_argument('-o', '--output', type=str, help='output', required=True)# 输出路径
    parser.add_argument('-ta', '--tasks', type=str, help='tasks', required=True)# task_shp
    args = parser.parse_args()

    if comm_rank == 0:
        datas = listdatas(args.input_csv)
        random.shuffle(datas)
        mpi_datas = divide(datas, comm_size)
    else:
        datas = None
        mpi_datas = None

    mpi_data_divide = comm.scatter(mpi_datas, root=0)

    for i in mpi_data_divide:
        df = pd.read_csv(i, header=0)# header=0表示使用数据列索引

        columns_data = df.columns.values.tolist()# 读取列索引

        columns_out = columns_data + ['TCC']
        df_out = pd.DataFrame(columns=columns_out)
        
        value_dict = {}
        for index, row in df.iterrows():
            for column in columns_data:
                value_dict[column] = [row[column]]
                
            value_dict['TCC'] = [extract(row["lon"], row["lat"], args.tasks, args.input_tcc)]
            df_out = df_out.append(pd.DataFrame(value_dict))
        
        df_out.to_csv(args.output + '/' + 'TCC_' + i.split('/')[-1], index=False, header=True)

    return

if __name__ == "__main__":
    main()