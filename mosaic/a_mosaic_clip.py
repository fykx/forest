def listdatas(pathin):
    import os
    
    a = []
    datas = os.listdir(pathin)
    for i in datas:
        if i[-4:] == '.tif':
            fn_i = pathin + '/' + i
            a.append(fn_i)
    return a


def mosaic_clip(tifs, mask, data_name, pathout):
    from osgeo import gdal,gdalconst,ogr,osr
    import os

    ds_tif = gdal.Open(tifs[0], gdalconst.GA_ReadOnly) # 输入文件
    band_tif = ds_tif.GetRasterBand(1)
    nodata = band_tif.GetNoDataValue()
    proj_tif = ds_tif.GetProjection()
    
    ds_mosaic = gdal.Warp('', tifs, format='MEM', srcSRS=proj_tif, dstSRS=proj_tif, srcNodata=nodata, dstNodata=nodata)

    ds_clip = gdal.Warp('', ds_mosaic, format='MEM', cutlineDSName=mask, cropToCutline=True, srcNodata=nodata, dstNodata=nodata)

    data_pathout = pathout + '/' + data_name

    if os.path.isdir(data_pathout):
            pass
    else:
        try:
            os.makedirs(data_pathout)
        except:
            pass

    out_data = data_pathout + '/' + data_name + '_mosiac_clip' + '.tif'

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
    parser.add_argument('-m', '--mask', type=str, help='mask', required=True)# mask
    args = parser.parse_args()

    if comm_rank == 0:
        #datas = ['dem', 'gain', 'loss', 'mask', 'slope','tcc_mean']
        datas = ['tcc_mean']
        random.shuffle(datas)
        mpi_datas = divide(datas, comm_size)
    else:
        datas = None
        mpi_datas = None

    mpi_data_divide = comm.scatter(mpi_datas, root=0)

    for i in mpi_data_divide:

        datas_path = args.input + '/' + i
        tile_datas = listdatas(datas_path)

        if len(tile_datas) > 0:
            mosaic_clip(tile_datas, args.mask, i, args.output)
        else:
            print(i)
        
    return

if __name__ == "__main__":
    main()

    

    





