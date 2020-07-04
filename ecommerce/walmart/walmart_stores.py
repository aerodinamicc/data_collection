import requests
import pandas as pd
import json
import os
import re
from tqdm import tqdm
import geopandas as gpd
from shapely.geometry import Point, Polygon


def scrape_stores(output_folder):
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
        
    os.chdir(output_folder)
    # we don't repeat scraping for the same store
    files_saved = os.listdir(output_folder)
    stores_saved = set()

    for f in files_saved:
        if f.endswith('.txt'):
            stores_saved.add(int(re.search('^(\d+)', f).group(0)))

    stores_saved = range(1, max(stores_saved))
    store_range = set(range(1, 10000))
    store_range = store_range.difference(stores_saved)

    #There are approximately 4759 Walmart stores in the US as stated in
    #Nevertheless the ceiling is higher because there are a lot of empty slots in between
    for store_id in tqdm(store_range, total=len(store_range)):
        if store_id in [518]:
            continue

        walmart_page = "https://www.walmart.com/store/"
        store_page = walmart_page + str(store_id) + '/'

        rq = requests.get(store_page)

        if rq.status_code != 200:
            print('Store with id %s is unavailable.' % (store_id))
            continue

        else:
            page_text = str(rq.text)
            start_phrase = '__WML_REDUX_INITIAL_STATE__ = '
            end_phrase = '</script>'
            st = page_text.find(start_phrase) + len(start_phrase)
            e = page_text.find(end_phrase, st) - 1
            js = page_text[st:e]

            with open(output_folder+ str(store_id) + '.txt', "w") as text_file:
                file = js.replace('\x81', '').replace('\x90', '').replace('\x9d', '').replace('\x8d', '')
                json.loads(js)
                text_file.write(file)


def safe_check_key_in_dict(dict, keys):
    level = 0
    value = dict

    while level < len(keys):
        if keys[level] in value.keys():
            value = value[keys[level]]
            level += 1
        else:
            return None

    return value


def clean_store_info(output_folder):
    stores = pd.DataFrame()
    
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
        
    os.chdir(output_folder)
    
    files_saved = os.listdir()
    for file in tqdm(files_saved):
        with open(file, 'r') as content_file:
            json_content = json.loads(content_file.read())

            services = []
            for s in safe_check_key_in_dict(json_content, ['store', 'primaryServices']):
                services.append((safe_check_key_in_dict(s, ['groupName']),
                                 safe_check_key_in_dict(s, ['displayName'])))

            secondary_services = []
            for s in safe_check_key_in_dict(json_content, ['store', 'secondaryServices']):
                secondary_services.append((safe_check_key_in_dict(s, ['groupName']),
                                           safe_check_key_in_dict(s, ['displayName'])))

            stores = stores.append({'store_name': safe_check_key_in_dict(json_content, ['store', 'displayName']),
                                    'store_id': safe_check_key_in_dict(json_content, ['store', 'id']),
                                    'store_type': safe_check_key_in_dict(json_content, ['store', 'storeType', 'name']),
                                    'is_open_24h': safe_check_key_in_dict(json_content, ['store', 'operationalHours', 'open24Hours']),
                                    'lon': safe_check_key_in_dict(json_content, ['store', 'geoPoint', 'longitude']),
                                    'lat': safe_check_key_in_dict(json_content, ['store', 'geoPoint', 'latitude']),
                                    'opening_date': safe_check_key_in_dict(json_content, ['store', 'openDate']),
                                    #'is_kiosk': safe_check_key_in_dict(json_content, ['store', 'kiosk']),
                                    #'is_deleted': safe_check_key_in_dict(json_content, ['store', 'deleted']),
                                    'postal_code': safe_check_key_in_dict(json_content, ['store', 'address', 'postalCode']),
                                    'street_address': safe_check_key_in_dict(json_content, ['store', 'address', 'streetAddress']),
                                    'city': safe_check_key_in_dict(json_content, ['store', 'address', 'city']),
                                    'state': safe_check_key_in_dict(json_content, ['store', 'address', 'state']),
                                    'country': safe_check_key_in_dict(json_content, ['store', 'address', 'country']),
                                    'can_order_custom_cakes_online': safe_check_key_in_dict(json_content, ['store', 'canOrderOnlineCustomCakes']),
                                    #'is_coming_soon': safe_check_key_in_dict(json_content, ['store', 'isComingSoon']),
                                    #'is_fedex_pickup_store': safe_check_key_in_dict(json_content, ['store', 'isFedExPickupStore']),
                                    #'is_relocation_soon':  safe_check_key_in_dict(json_content, ['store', 'isRelocatingSoon']),
                                    'primary_services': services,
                                    'secondary_services': secondary_services},
                                   ignore_index=True)

    return stores


def save_files(stores):
    if os.path.exists(output_folder)
        os.mkdir(output_folder)

    os.chdir(output_folder)
    stores.to_csv('walmart_store.tsv', index=False, sep='\t')

    primary_services_exploded = stores.explode('primary_services')
    primary_services_exploded[['ps_group_name', 'primary_service']] = pd.DataFrame(
        primary_services_exploded['primary_services'].tolist(), index=primary_services_exploded.index)
    primary_services_exploded.drop(columns=['primary_services'], inplace=True)
    primary_services_exploded.to_csv('walmart_store_primary_services_exploded.tsv', index=False, sep='\t')

    secondary_services_exploded = stores.explode('secondary_services')
    secondary_services_exploded[['ss_group_name', 'secondary_service']] = pd.DataFrame(
        secondary_services_exploded['secondary_services'].tolist(), index=secondary_services_exploded.index)
    secondary_services_exploded.drop(columns=['secondary_services'], inplace=True)
    secondary_services_exploded.to_csv('walmart_store_secondary_services_exploded.tsv', index=False, sep='\t')


def convert_to_geodf(df, lon_column='longitude', lat_column='latitude'):
    crs = {'init': 'epsg:4326'}
    df['coor'] = df[[lon_column, lat_column]].values.tolist()
    df['coor'] = df['coor'].apply(Point)
    df = gpd.GeoDataFrame(df,
                          crs=crs,
                          geometry='coor')
    df.to_crs={'init': 'epsg:4326'}

    # import pdb; pdb.set_trace()
    df.drop(columns=['primary_services', 'secondary_services'], inplace=True)

    create_buffers(df, 10000)
    create_buffers(df, 20000)

    return df


def create_buffers(df, buffer_in_meters):
    buffers = gpd.GeoDataFrame(df.coor.buffer(0.00001 * buffer_in_meters))
    buffers.rename(columns={0: 'geometry'}, inplace=True)

    buffers = pd.concat([buffers, df[['store_id']]], axis=1)
    buffers.to_file(output_folder + 'shp/' + 'walmart_stores_{}km_buffer.shp'.format(round(buffer_in_meters/1000)))


def main(output_folder):
    scrape_stores(output_folder)
    stores = clean_store_info(output_folder)
    save_files(stores, output_folder)
    convert_to_geodf(stores, 'lon', 'lat')


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-output_folder', required=True, help="/?")
	parsed = parser.parse_args()
	output_folder = parsed.output_folder + '/' 
    main(output_folder)




