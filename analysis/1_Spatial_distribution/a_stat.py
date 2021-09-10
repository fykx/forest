def read_tiles(tasks):
    from osgeo import gdal,ogr,osr

    tasks_ds = ogr.Open(tasks, 0)
    tasks_lyr = tasks_ds.GetLayer(0)

    tiles = []

    for tile_feat in tasks_lyr:
        tiles.append(tile_feat.GetField('tag'))
        
    del tasks_ds

    return tiles


def stat(tiles, pathin, pathout):
    import pandas as pd
    import numpy as np

    three_d = []

    for tile in tiles:
        csv = pathin + '/' + tile + '.csv'
        df = pd.read_csv(csv, header=0)# header=0表示使用数据列索引

        three_d.append(df.values)

    three_d_array = np.array(three_d)

    value_counts = three_d_array.shape[1]

    columns_out = ['altitude', 'mean']
    df_out = pd.DataFrame(columns=columns_out)

    for i in range(value_counts):
        altitude = three_d_array[0,i,0]
        value_array = three_d_array[:,i,1]

        float_value_array = value_array.astype(np.float64)

        mask_value_array = np.ma.masked_where(float_value_array==-9999.0, float_value_array)

        if mask_value_array.count() > 0:
            value_mean = mask_value_array.mean()
        else:
            value_mean = -9999

        value_dict = {}

        value_dict['altitude'] = [altitude]
        value_dict['mean'] = [value_mean]
        df_out = df_out.append(pd.DataFrame(value_dict))

    df_out.to_csv(pathout + '/' + 'stat' + '.csv', index=False, header=True)

    return


def main():
    tasks = r'/public/home/mfeng/jwang/paper/10_ne/data/tasks/tasks.shp'
    pathin = r'/public/home/mfeng/jwang/paper/10_ne/result/1_Spatial_distribution/reslut/dem/1_tiles'
    pathout = r'/public/home/mfeng/jwang/paper/10_ne/result/1_Spatial_distribution/reslut/dem/2_stat'
    
    tiles = read_tiles(tasks)
    stat(tiles, pathin, pathout)
    return


if __name__ == "__main__":
    main()