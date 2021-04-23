'''
Descripttion: 森林变化参数提取，读取csv文件点（经度、纬度），并提取各森林变化参数，输出csv文件
version: 0.1
Author: Jianbang Wang
Date: 2021-03-01
'''

def listdatas(pathin):
    import os
    a = []
    datas = os.listdir(pathin)
    for i in datas:
        if i.endswith('.tif'):
            fn_i = pathin + '/' + i
            a.append(fn_i)
    return a

'''判断与该点相交的多边形并提取值'''
def filter(lon, lat, tasks, pathin, pathout):
    from osgeo import ogr, gdal
    import pandas as pd

    '''打开tasks shp'''
    ds_tasks = ogr.Open(tasks, 0)
    lyr_tasks = ds_tasks.GetLayer(0)

    point_geometry = ogr.Geometry(ogr.wkbPoint)
    point_geometry.AddPoint(lon, lat)

    for task_feat in lyr_tasks:
        task_geometry = task_feat.geometry()

        row_value = {}

        if task_geometry.Intersects(point_geometry) == True:# 判断是否接触（接触+包含）（思路二：可将有接触的多个tile镶嵌至虚拟栅格，然后在虚拟栅格中读取值）

            tag = task_feat.GetField('tag')
            pathin_data = pathin + '/' + tag[0:5] + '/' + tag[-4:] + '/' + tag

            tifs = listdatas(pathin_data)

            for tif in tifs:
                in_ds = gdal.Open(tif)
                in_band = in_ds.GetRasterBand(1)# 波段索引从1开始
                in_data = in_band.ReadAsArray()

                xsize = in_band.XSize# 列
                ysize = in_band.YSize# 行

                transform = in_ds.GetGeoTransform()
                xOrigin = transform[0]#左上角位置
                yOrigin = transform[3]#左上角位置
                pixelWidth = transform[1]#像元宽度
                pixelHeight = transform[5]#像元高度

                xOffset = int((lon-xOrigin)/pixelWidth)
                yOffset = int((lat-yOrigin)/pixelHeight)

                if xOffset < xsize and yOffset < ysize:# 如果点落在右边和下边的话，不读取这个数据
                    value = in_data[yOffset, xOffset]
                    row_value[tif.split('/')[-1].split('.')[0].replace(tif.split('/')[-1].split('.')[0].split('_')[0]+'_', '')] = value
                    del in_ds
                
                else:
                    break
        
        if len(row_value) > 0:# 如果数据读已经取成功停止遍历多边形
            break
    return row_value
            

def main():
    import pandas as pd

    csv = r'/public/home/mfeng/jwang/work_tcc/test_1/china/file01/scene01/comp01/change0a/0301/data/data.csv'
    tasks = r'/public/home/mfeng/jwang/work_tcc/test_1/china/file01/scene01/comp01/change0a/0301/data/tasks.shp'
    pathin = r'/public/home/mfeng/jwang/work_tcc/test_1/china/file01/scene01/comp01/change0a/data'
    pathout = r'/public/home/mfeng/jwang/work_tcc/test_1/china/file01/scene01/comp01/change0a/0301/out'

    df = pd.read_csv(csv, header=0)# header=0表示使用数据列索引

    out_csv = {}

    for row in df.itertuples(index=True, name='Pandas'):# 遍历df每一行
        lon = getattr(row, "lon")
        lat = getattr(row, "lat")
        weight = getattr(row, "weight")
        tag = getattr(row, "tag")
        loss = getattr(row, "loss")
        gain = getattr(row, "gain")
        name_ID = getattr(row, "name_ID")

        row_value = filter(lon, lat, tasks, pathin, pathout)

        row_value['weight'] = weight
        row_value['tag'] = tag
        row_value['loss'] = loss
        row_value['gain'] = gain
        row_value['lon'] = lon
        row_value['lat'] = lat

        out_csv[name_ID] = row_value
    
    frame = pd.DataFrame(out_csv).T
    frame.index.name = 'name_ID'
    frame.to_csv(pathout + '/' + 'result.csv')
    return

if __name__ == "__main__":
    main()