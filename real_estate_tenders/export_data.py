import pandas as pd
import random
import math
from transliterate import slugify
import datetime


def get_coor(row, lon_col, lat_col):
    return row[lon_col], row[lat_col]


def dispersing_clusters(records, lon_col, lat_col):
    records['coor'] = records.apply(get_coor, args=[lon_col, lat_col], axis=1)

    if 'is_appr_coor' not in records.columns:
        records['is_appr_coor'] = 'False'

    reps = records.groupby(['coor']).size().reset_index(name='reps').sort_values(by='reps', ascending=False)
    #print(reps.shape[0])

    for cluster in reps[reps['reps'] > 10]['coor'].unique():
        n = reps[reps['coor'] == cluster]['reps'].values[0]
        idxs = records[records['coor'] == cluster].index

        circle_r = 0.01
        alpha = math.pi * (3 - math.sqrt(5))    # the "golden angle"
        phase = random.random() * 2 * math.pi
        points = []
        for k in range(n):
            theta = k * alpha + phase
            r = circle_r * math.sqrt(float(k)/n)
            points.append((r * math.cos(theta) + cluster[0], r * math.sin(theta) + cluster[1]))

        new_coor = pd.DataFrame(points, columns=['lon', 'lat'])
        records.loc[idxs, 'display_lon'] = new_coor['lon'].values
        records.loc[idxs, 'display_lat'] = new_coor['lat'].values
        records.loc[idxs, 'is_appr_coor'] = True

    return records


def export_files(is_transliterate=True):
    si = pd.read_csv('si_rec.csv', sep='\t')
    si = dispersing_clusters(si, 'display_lon', 'display_lat')
    nap = pd.read_csv('nap_rec.csv', sep='\t')
    nap = dispersing_clusters(nap, 'lon', 'lat')

    si = si[['area', 'place', 'court_short', 'price', 'type', 'display_lon',
           'display_lat', 'is_appr_coor', 'period_start', 'period_end',
             'link', 'ideal_parts']]
    si.rename(columns={'place': 'address', 'display_lon': 'lon', 'display_lat': 'lat', 'court_short': 'region'}, inplace=True)
    si['source'] = 'https://sales.bcpea.org/'

    nap = nap[['area', 'address', 'region', 'price', 'type', 'lon',
           'lat', 'is_appr_coor', 'period_start', 'period_end',
             'link', 'ideal_parts']]
    nap['source'] = 'https://sales.nra.bg'

    export = pd.concat([si, nap])
    export['price'] = export['price'].astype(int)
    export = export[~pd.isnull(export['lon'])]

    print("len before date filtering is {}".format(export.shape[0]))
    date_filter = str(datetime.datetime.now().date())
    export = export[export['period_end'].apply(lambda x: x > date_filter)]
    print("len before date filtering is {}".format(export.shape[0]))

    if is_transliterate:
        export['address'] = export['address'].apply(lambda x: slugify(x) if str(x) not in ['nan', ''] else x)
        export['region'] = export['region'].apply(lambda x: slugify(x) if str(x) not in ['nan', ''] else x)
        export['type'] = export['type'].apply(lambda x: slugify(x) if str(x) not in ['nan', ''] else x)
        export.to_csv('export_tr.tsv', sep='\t', index=False)
    else:
        export.to_csv('export.tsv', sep='\t', index=False)


if __name__ == '__main__':
    export_files()





