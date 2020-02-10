import math
import pandas as pd
import requests
from pymongo import MongoClient

client = MongoClient("mongodb://localhost/geo")
db = client.get_database()

def asGeoJSON(lat,lng):
    try:
        lat = float(lat)
        lng = float(lng)
        if not math.isnan(lat) and not math.isnan(lng):
            return {
                "type":"Point",
                "coordinates":[lng,lat]
            }
    except Exception:
        print("Invalid data")
        return None

def geocode(address):
    data = requests.get(f"https://geocode.xyz/{address}?json=1").json()
    return {
        "type":"Point",
        "coordinates":[float(data["longt"]),float(data["latt"])]
    }


def withGeoQuery(location,maxDistance=10000,minDistance=0,field="location"):
    return {
       field: {
         "$near": {
           "$geometry": location if type(location)==dict else geocode(location),
           "$maxDistance": maxDistance,
           "$minDistance": minDistance
         }
       }
    }

def finding(starbucksDistance = 500, airportDistance = 20000):
    lista = db["clean"].find({},{"name": 1, "_id": 0}).limit(1000)
    newDf = pd.DataFrame()
    for location in lista:
        locat = list(db["clean"].find({"$and":[{"name": str(location["name"])}, {"category_code": "web"}]}, {"name": 1, "location": 1, "_id":0}))
        try:
            placeLoc = locat[0]["location"]
            placeName = locat[0]["name"]
            starb = withGeoQuery(placeLoc, maxDistance = starbucksDistance)
            airp = withGeoQuery(placeLoc, maxDistance = airportDistance)
            starbDis = len(list(db["starbucks"].find(starb)))
            airpDis = len(list(db["airports"].find(airp)))
            if starbDis and airpDis > 0:
                possibleLoc = {
                    "name": locat[0]["name"],
                    "Starbucks_Close": starbDis,
                    "Airports_Close": airpDis,
                    "Longitude": locat[0]["location"]["coordinates"][0],
                    "Latitude": locat[0]["location"]["coordinates"][1]
                }
                newDf = newDf.append(possibleLoc, ignore_index=True)
        except:
            pass
    newDf = newDf.drop_duplicates(["name"])
    return newDf