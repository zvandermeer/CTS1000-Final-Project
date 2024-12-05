from flask_app.geojson import loadGeoJSON

def calculateSentimentData(rows):
    geoData = loadGeoJSON()

    locationTotals = {}
    yearTotals = {}

    for item in rows:
        if item['countryCode'] != None:
            if item['countryCode'] in locationTotals:
                locationTotals[item['countryCode']][item['sentiment']] += 1
            else:
                locationTotals[item['countryCode']] = {}
                locationTotals[item['countryCode']]['positive'] = 0
                locationTotals[item['countryCode']]['neutral'] = 0
                locationTotals[item['countryCode']]['negative'] = 0

                locationTotals[item['countryCode']][item['sentiment']] += 1

        if item['stateCode'] != None and item['stateCode'] != '':
            if item['stateCode'] in locationTotals:
                locationTotals[item['stateCode']][item['sentiment']] += 1
            else:
                locationTotals[item['stateCode']] = {}
                locationTotals[item['stateCode']]['positive'] = 0
                locationTotals[item['stateCode']]['neutral'] = 0
                locationTotals[item['stateCode']]['negative'] = 0

                locationTotals[item['stateCode']][item['sentiment']] += 1

        if item['creationDate'][:4] in yearTotals:
            yearTotals[item['creationDate'][:4]][item['sentiment']] += 1
        else:
            yearTotals[item['creationDate'][:4]] = {}
            yearTotals[item['creationDate'][:4]]['positive'] = 0
            yearTotals[item['creationDate'][:4]]['neutral'] = 0
            yearTotals[item['creationDate'][:4]]['negative'] = 0

            yearTotals[item['creationDate'][:4]][item['sentiment']] += 1

    yearData = {}

    for year in yearTotals:
        totalPoints = yearTotals[year]['positive'] + yearTotals[year]['neutral'] + yearTotals[year]['negative']
        mean = (yearTotals[year]['positive'] - yearTotals[year]['negative']) / totalPoints

        yearData[str(year)] = str(mean)

    for location in locationTotals:
        totalPoints = locationTotals[location]['positive'] + locationTotals[location]['neutral'] + locationTotals[location]['negative']
        mean = (locationTotals[location]['positive'] - locationTotals[location]['negative']) / totalPoints

        if location[:3] == "US-":
            for item in geoData['geoUS']['features']:
                if item['id'].lower() == location.lower():
                    item['properties']['meanSentiment'] = mean
                    break

        elif location[:3] == "CA-":
            for item in geoData['geoCA']['features']:
                if item['id'].lower() == location.lower():
                    item['properties']['meanSentiment'] = mean
                    break

        else:
            for item in geoData['geoWORLD']['features']:
                if item['id'].lower() == location.lower():
                    item['properties']['meanSentiment'] = mean
                    break

    return [geoData, yearData]