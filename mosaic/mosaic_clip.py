def listdatas(pathin):
    import os

    a = []
    datas = os.listdir(pathin)
    for i in datas:
        if i[-4:] == '.tif':
            fn_i = pathin + '/' + i
            a.append(fn_i)
    return a

def mosaic_clip(tifs, mask, pathout):
    from osgeo import gdal,gdalconst,ogr,osr
    import os

    ds_tif = gdal.Open(tifs[0], gdalconst.GA_ReadOnly) # 输入文件
    band_tif = ds_tif.GetRasterBand(1)
    nodata = band_tif.GetNoDataValue()
    proj_tif = ds_tif.GetProjection()
    
    ds_mosaic = gdal.Warp('', tifs, format='MEM', srcSRS=proj_tif, dstSRS=proj_tif, srcNodata=nodata, dstNodata=nodata)

    ds_clip = gdal.Warp('', ds_mosaic, format='MEM', cutlineDSName=mask, cropToCutline=True, srcNodata=nodata, dstNodata=nodata)

    out_data = pathout + '/' + 'dem_mosaic_clip.tif'

    driver = gdal.GetDriverByName('GTiff')
    driver.CreateCopy(out_data, ds_clip, strict=1, options=["TILED=YES", "COMPRESS=LZW"])

    del ds_tif
    del ds_mosaic
    del ds_clip
    return


def main():
    mask = r'/public/home/mfeng/cwang/Himalaya/data/shp/Himalaya_albers.shp'
    pathin = r'/public/home/mfeng/cwang/Himalaya/data/data_albers_tiles/result/dem'
    pathout = r'/public/home/mfeng/cwang/Himalaya/data/data_albers_tiles/result/dem/mosaic'

    tifs = listdatas(pathin)
    mosaic_clip(tifs, mask, pathout)


    return

if __name__ == "__main__":
    main()

    

    





