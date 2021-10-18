from collections import defaultdict
import geocoder


def lookup_location(lat: float, lon: float) -> dict:
    """
    Adds new locations to the database.

    :param lat: Latitude of the new location.
    :param lon: Longitude of the new location.
    :return: Dictionary with data for one location
    """
    location = defaultdict(list)
    g = geocoder.osm(location=(lat, lon), method='reverse')
    assert g.ok, f"Error response: {g.response.text}"
    location['latitude'].append(lat)
    location['longitude'].append(lon)
    location['place_id'].append(g.place_id)
    location['osm_id'].append(g.osm_id)
    location['country_code'].append(g.country_code.upper())
    location['country'].append(g.country)
    location['region'].append(g.region)
    location['state_name'].append(g.state)
    location['city'].append(g.city)
    location['town'].append(g.town)
    location['village'].append(g.village)
    location['suburb'].append(g.suburb)
    location['quarter'].append(g.quarter)
    location['neighborhood'].append(g.neighborhood)
    location['street'].append(g.street)
    location['postal'].append(g.postal)
    location['address'].append(g.address)
    return location
