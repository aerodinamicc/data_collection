import pandas as pd
import crawl_google_for_certain_cities as crawl


def match_place_names_to_location():
    pl = pd.read_csv('si_records.csv' ,sep='\t')
    pl['place_lower'] = pl['place'].apply(lambda x: x.lower().strip())
    pl['court_short_lower'] = pl['court_short'].apply(lambda x: x.lower().strip())

    new_places_ind = pl[pl.lon.map(str).apply(lambda x: x == 'nan' or x == '')].index
    print(len(new_places_ind))
    new_places = pl.iloc[new_places_ind]
    print(pl.shape)

    coors = pd.read_csv('places_decoded.csv', sep='\t')
    coors['region'].iloc[coors[coors['mun'].apply(lambda x: x.strip()) == 'София-град'].index] = 'София-град'
    coors['region_lower'] = coors['region'].apply(lambda x: x.lower().strip())
    coors['name_lower'] = coors['name'].apply(lambda x: x.lower().strip())
    coors['settlement_type'] = coors['settlement_type'].apply(lambda x: x.strip())

    new_places = match_messy_name_to_location(new_places, coors)
    new_places = crawl.look_for_more_precise_locations(new_places)

    pl = pd.concat([new_places, pl])
    pl = pl.drop_duplicates('page_id', keep='first')

    if 'display_lon' not in pl.columns:
        pl['display_lon'] = None
        pl['display_lat'] = None
        pl['display_lon'] = pl.apply(get_display_lon, axis=1)
        pl['display_lat'] = pl.apply(get_display_lat, axis=1)
    else:
        idxs = pl[pl['display_lon'].apply(lambda x: str(x) in ['nan', ''])].index
        pl.loc[idxs, 'display_lon'] = pl.iloc[idxs].apply(get_display_lon, axis=1)
        pl.loc[idxs, 'display_lat'] = pl.iloc[idxs].apply(get_display_lat, axis=1)


    pl.drop(columns=['place_lower', 'court_short_lower'], inplace=True)
    pl.to_csv('si_records.csv', sep='\t', index=False)


def get_display_lon(row):
    if str(row['potential_lon']) not in ['nan', '']:
        return row['potential_lon']

    return row['lon']


def get_display_lat(row):
    if str(row['potential_lat']) not in ['nan', '']:
        return row['potential_lat']

    return row['lat']


def match_messy_name_to_location(pl, coors):
    for ind, row in pl.iterrows():
        reg = row['court_short_lower']
        place_full_name = row['place_lower']
        sub_coors = coors[coors['region_lower'] == reg]
        villages = sub_coors[sub_coors['settlement_type'] == 'с']['name_lower'].tolist()
        towns = sub_coors[sub_coors['settlement_type'] == 'гр']['name_lower'].tolist()

        appropriate_index = 0
        str_min_ind = 1000
        isMatched = False
        for village in villages:
            if village in place_full_name and place_full_name.find(village) < str_min_ind:
                str_min_ind = place_full_name.find(village)
                appropriate_index = \
                sub_coors[(sub_coors['name_lower'] == village) & (sub_coors['settlement_type'] == 'с')].index[0]
                isMatched = True

        for town in towns:
            if town in place_full_name and place_full_name.find(town) < str_min_ind:
                str_min_ind = place_full_name.find(town)
                # import pdb; pdb.set_trace()
                appropriate_index = \
                sub_coors[(sub_coors['name_lower'] == town) & (sub_coors['settlement_type'] == 'гр')].index[0]
                isMatched = True

        if isMatched:
            # import pdb; pdb.set_trace()
            pl.loc[ind, 'lon'] = coors.iloc[appropriate_index]['lon']
            pl.loc[ind, 'lat'] = coors.iloc[appropriate_index]['lat']
            pl.loc[ind, 'identified_location'] = coors.iloc[appropriate_index]['name_lower']

    return pl


if __name__ == '__main__':
    match_place_names_to_location()
