import pandas as pd


def match_place_names_to_location(records):
    records['place_lower'] = records['place'].apply(lambda x: x.lower().strip())
    records['court_short_lower'] = records['court_short'].apply(lambda x: x.lower().strip())

    coors = pd.read_csv('places_decoded.csv', sep='\t')
    coors['region'].iloc[coors[coors['mun'].apply(lambda x: x.strip()) == 'София-град'].index] = 'София-град'
    coors['region_lower'] = coors['region'].apply(lambda x: x.lower().strip())
    coors['name_lower'] = coors['name'].apply(lambda x: x.lower().strip())
    coors['settlement_type'] = coors['settlement_type'].apply(lambda x: x.strip())

    records['lon'] = None
    records['lat'] = None
    records['city_clean'] = None
    records['mun_clean'] = None

    records = match_messy_name_to_location(records, coors)

    records.drop(columns=['place_lower', 'court_short_lower'], inplace=True)
    return records


def match_messy_name_to_location(records, coors):
    for ind, row in records.iterrows():
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
                appropriate_index = \
                sub_coors[(sub_coors['name_lower'] == town) & (sub_coors['settlement_type'] == 'гр')].index[0]
                isMatched = True

        if isMatched:
            records.loc[ind, 'lon'] = coors.iloc[appropriate_index]['lon']
            records.loc[ind, 'lat'] = coors.iloc[appropriate_index]['lat']
            records.loc[ind, 'city_clean'] = coors.iloc[appropriate_index]['name']
            records.loc[ind, 'mun_clean'] = coors.iloc[appropriate_index]['mun']

    return records
