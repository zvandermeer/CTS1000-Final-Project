import json

def loadGeoJSON():
    returnData = {}

    with open('flask_app/geojson/ca.json') as f:
        returnData['geoCA'] = json.load(f)

    with open('flask_app/geojson/us.json') as f:
        returnData['geoUS'] = json.load(f)

    with open('flask_app/geojson/world.json') as f:
        returnData['geoWORLD'] = json.load(f)

    return returnData
    