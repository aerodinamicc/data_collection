## Run
```virtualenv real_estate```

## Then activate the env:
```real_estate\Scrips\activate```

## Install dependencies
```pip install -r requirements.txt```

## Additional dependencies for geopandas
```pipwin install shapely
pipwin install gdal
pipwin install fiona
pipwin install pyproj
pipwin install six
pipwin install rtree
pipwin install geopandas```

## To scrape holmes.bg run
```python .\holmes.bg.py -current_date 0626```

## To scrape imoti.bg run
```python .\imoti.bg.py -current_date 0626```
