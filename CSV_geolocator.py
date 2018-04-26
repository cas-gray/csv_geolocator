#!/usr/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import csv
import json
import math
import os
import itertools

import googlemaps
from datetime import datetime


class CSVGeolocator(object):
    "Geolocate places using googlemaps API for geocoding."

    JSON_CACHE_FILE = os.path.dirname(os.path.realpath(__file__)) + "/cache.json"

    EMPTY_COLUMN_CHARACTERS = ["-", " ", ""]

    def __init__(self, input_csv, out_csv, api_key, search_options):
        """Build our Geolocator

        Args:
            input_csv (str): the input filename
            out_csv (str): the output filename
            api_key (str): the API key
            search_options (dict): the search search_options
        """
        self.gmaps_client = googlemaps.Client(key=api_key)
        self.search_options = search_options

        # Try loading the cache
        try:
            with open(CSVGeolocator.JSON_CACHE_FILE, 'r') as cache_file:
                json_cache = json.load(cache_file)
                self.discovered_addrs = json_cache["discovered_addrs"]
                self.no_result_list = json_cache["no_result_list"]
        except:
            self.discovered_addrs = {}
            self.no_result_list = []

        self.input_csv = input_csv
        self.out_csv = out_csv

        # Run the geolocation, catching API failures
        try:
            self.geolocate_csv()
        finally:
            with open(CSVGeolocator.JSON_CACHE_FILE, 'w') as cache_file:
                json_data = {"discovered_addrs": self.discovered_addrs,
                             "no_result_list": self.no_result_list}
                json.dump(json_data, cache_file)

    def geolocate_csv(self):
        with open(self.out_csv, 'w') as out_file:
            with open(self.input_csv) as csv_file:
                reader = csv.DictReader(csv_file)
                csv_keys = reader.fieldnames + ['lat', 'lng', 'warnings']
                print(csv_keys)
                writer = csv.DictWriter(out_file, fieldnames=csv_keys)
                for row in reader:
                    # remove things where there are extra commas
                    if None in row:
                        row.pop(None)
                    addr_snips = []
                    print(row, self.search_options["csv_column_search_order"])
                    for key in self.search_options["csv_column_search_order"]:
                        addr_snip = row[key].strip()
                        if addr_snip not in CSVGeolocator.EMPTY_COLUMN_CHARACTERS:
                            addr_snips.append(addr_snip.lower())
                    out_row = row
                    lat, lng, warnings = self._geolocate(addr_snips)
                    if math.isnan(lat) or math.isnan(lng):
                        out_row.update({"lat": '-',
                                        "lng": '-',
                                        "warnings": warnings})
                    else:
                        out_row.update({"lat": lat,
                                        "lng": lng,
                                        "warnings": warnings})
                    writer.writerow(out_row)



    def _geolocate(self, addr_snips):
        """Use googlemaps Geocoding to get the latitude and longitude of a city.

        Our strategy is to grab, the smallest city (3 for preference), and
        search that alone first, then 'largest, smallest' then 'largest'.

        This stragey is used because the change of borders over time make it so
        that city3 may no longer be located in city1, (e.g. Fokino is no
        longer in Orjol). However, if we can't find it, adding context may
        help. Finally, just city1 on it's own provides a fallback, and usually
        won't require a search as it will have already been done.

        NOTES:
        We do bounding uing viewport biasing and country code biasing, *not*
        partial address matching. The reason for this is that, due to historical
        context of the data, the region may not be relied upon (see above), and
        the googlemaps API does not support mutiple country matches:

            'If the request contains multiple component filters, the API
            evaluates them as an AND, not an OR. For example, if the request
            includes multiple countries components=country:GB|country:AU, the
            API looks for locations where country=GB AND country=AU, and returns
            ZERO_RESULTS.
        """
        warnings = ""
        composed_addresses = []
        # get every combination of addrress snippets up to max depth. prioritizing first including higher res info,
        #   then not including lower res info.
        #   e.g. "123 fake street", "123 fake street, nowheresville", "123 fake street, usa", "nowheresville"

        for depth in range(self.search_options["search_depth"]):
            composed_addresses += itertools.combinations(addr_snips, depth + 1)
        composed_addresses.sort(key=lambda snips: (min([addr_snips.index(x) for x in snips]),
                                                   max([addr_snips.index(x) for x in snips])))

        composed_addresses = [", ".join(snips_list) for snips_list in composed_addresses]
        print(composed_addresses)

        for attempt_index, attempted_address in enumerate(composed_addresses):

            # the highest info address only exists in the first half of searches
            if attempt_index > 2 ** (len(addr_snips) - 1):
                warnings += "WARNING: Using fallbacks, loss of fidelity!"
            print(attempted_address)

            #if we have it cached, no need to search
            if attempted_address in self.discovered_addrs:
                lat, lng = self.discovered_addrs[attempted_address]
                return lat, lng, warnings

            # if we know searching won't work, skip this location
            if attempted_address in self.no_result_list:
                continue
            else:
                # encode a search keyword Args
                gmaps_kwargs = {}
                if "search_tld" in self.search_options:
                    gmaps_kwargs["region"] = self.search_options["search_tld"]
                if "search_bounds" in self.search_options:
                    search_bounds = self.search_options["search_bounds"]
                    gmaps_bounds = {'northeast': {'latitude': search_bounds['latitude'][0],
                                                  'longitude': search_bounds['longitude'][0]},
                                    'southwest': {'latitude': search_bounds['latitude'][1],
                                                  'longitude': search_bounds['longitude'][1]},
                                    }
                    gmaps_kwargs["bounds"] = gmaps_bounds
                geocode_result = self.gmaps_client.geocode(attempted_address,
                                                           **gmaps_kwargs)

                if geocode_result:
                    location = geocode_result[0]['geometry']['location']
                    lat = float(location[u'lat'])
                    lng = float(location[u'lng'])
                    #cache result
                    self.discovered_addrs[attempted_address] = (lat, lng)
                    return lat, lng, warnings
                else:
                    warnings += "No result found for '{}'; ". format(attempted_address)
                    self.no_result_list.append(attempted_address)
        # if we failed, return nans
        warnings += "Search Failed!;"
        self.no_result_list.append(attempted_address)
        return float('nan'), float('nan'), warnings


def main():
    argparser = argparse.ArgumentParser("Geolocate all the Things!")
    argparser.add_argument("-f", "--csv_file",
                           help="A file containing locations to search in csv format")
    argparser.add_argument("-o", "--out_file", default="out.csv",
                           help="A copy of the input file, with added columns for latitude and longitude")
    argparser.add_argument("-c", "--config",
                           help="The config file")

    args = argparser.parse_args()

    with open(args.config) as config_file:
        config = json.load(config_file)
        api_key = config.pop("api_key")

    csv_locator = CSVGeolocator(args.csv_file, args.out_file, api_key, config)


if __name__ == "__main__":
    main()
