#!/usr/bin/env python3
"""
Coronaline cli script, please read README.md
"""

import numpy as np
import pandas as pd
import requests
import datetime as dt
import geopy.distance
#import tabulate
import json
import sys
import os
#import pdb

GOV_CASE_LOC_URL = "https://gisweb.azureedge.net/Points.json"

#def display(df):
#    print(tabulate.tabulate(df.head(10), headers='keys', tablefmt='fancy_grid'))

def downloadFile(url, path):
    """
    Download a file and save it.
    """
    print(f"Requesting {url}")
    res = requests.get(url, stream=True)
    length = res.headers.get("content-length")
    print(f"Downloading")
    with open(path, 'wb') as f:
        if length is None:
            print("No content length.")
            f.write(res.content)
        else:
            length = int(length)
            dl_length = 0
            print(f"Total size: {length}")
            for data in res.iter_content(chunk_size=4096):
                dl_length += len(data)
                f.write(data)
            print(f"Downloaded: {dl_length}")

def transformLocationHistory(loch_file):
    loch_data = json.load(open(loch_file, "r"))
    loch_df = pd.DataFrame.from_dict(loch_data)
    print(f"Number of Google location history points loaded: {loch_df.shape[0]:,}.")
    loch_df['timestamp'] = loch_df.locations.map(lambda x: int(x['timestampMs'])) 
    loch_df['longitude'] = loch_df.locations.map(lambda x: x['longitudeE7']/1e7)
    loch_df['latitude'] = loch_df.locations.map(lambda x: x['latitudeE7']/1e7)
    loch_df['accuracy'] = loch_df.locations.map(lambda x: x['accuracy'])
    loch_df['datetime'] = loch_df.timestamp.map(lambda x: dt.datetime.fromtimestamp(x/1000))
    loch_df= loch_df.drop(labels=['locations'], axis=1, inplace=False)
    print(f"Timeline: from {loch_df.datetime.min().strftime('%d/%m/%Y %H:%M')} to {loch_df.datetime.max().strftime('%d/%m/%Y %H:%M')}")
    print(f"Accuracy: min {loch_df.accuracy.min()}, max {loch_df.accuracy.max()}")
    # Debug print df.
    #display(loch_df)
    return loch_df

def transformMOHData(moh_file):
    def _convertTime(time, index, start_timestamp_ms, end_timestamp_ms):
        """
        A horrible function to try and convert the disguising MOH time data to something useful.
        """
        try:
            parsed_time = dt.datetime.strptime(time.split('-')[index].strip(), "%H:%M").time()
            parsed_start_date = dt.datetime.fromtimestamp(start_timestamp_ms/1000).date()
            parsed_end_date = dt.datetime.fromtimestamp(end_timestamp_ms/1000).date()
            if index == 0:
                parsed_date = parsed_start_date
            else:
                parsed_date = parsed_end_date

            # Dumb MOH programmers use 00:00 to denote midnight without changing the date
            # So, if we are looking for the end date, and the time is 00:00 and the dates did not change, fix it.
            # This still does not fix the "09:00 - 02:00" problem.
            if (index == 1) and (parsed_time == dt.time(0,0)) and (parsed_start_date == parsed_end_date):
                delta = dt.timedelta(days=1)
            else:
                delta = dt.timedelta(days=0)

            return dt.datetime.combine(parsed_date + delta, parsed_time)
        except Exception as ex:
            # A date of 01/01/2262 marks a parse error.
            return dt.datetime(2262,1,1,0,0,0)

    moh_data = json.load(open(moh_file,  "r", encoding="utf8")) 
    moh_df = pd.DataFrame.from_dict(moh_data)  
    print(f"Number of MOH points loaded: {moh_df.shape[0]:,}.")
    moh_df['id'] = moh_df['features'].map(lambda x: x['id'])
    moh_df['longitude'] = moh_df['features'].map(lambda x: x['geometry']['coordinates'][0])
    moh_df['latitude'] = moh_df['features'].map(lambda x: x['geometry']['coordinates'][1])
    moh_df['type'] = moh_df['features'].map(lambda x: x['geometry']['type'])
    moh_df['iname'] = moh_df['features'].map(lambda x: x['properties']['Name'])
    moh_df['place'] = moh_df['features'].map(lambda x: x['properties']['Place'])
    moh_df['comments'] = moh_df['features'].map(lambda x: x['properties']['Comments'])
    moh_df['start_datetime'] = moh_df['features'].map(lambda x: _convertTime(x['properties']['stayTimes'], 0, x['properties']['fromTime'], x['properties']['toTime']))
    moh_df['end_datetime'] = moh_df['features'].map(lambda x: _convertTime(x['properties']['stayTimes'], 1, x['properties']['fromTime'], x['properties']['toTime']))
    # Debug datetime columns, not used for any referencing.
    moh_df['debug_stayTimes'] = moh_df['features'].map(lambda x: x['properties']['stayTimes'])
    moh_df['debug_fromTime'] = moh_df['features'].map(lambda x: dt.datetime.fromtimestamp(x['properties']['fromTime']/1000))
    moh_df['debug_toTime'] = moh_df['features'].map(lambda x: dt.datetime.fromtimestamp(x['properties']['fromTime']/1000))
    moh_df = moh_df.drop(labels=['features'], axis=1, inplace=False)

    # Dispay subset of data and count and subset of bad time information
    print(f"MOH data: from {moh_df.start_datetime.min().strftime('%d/%m/%Y %H:%M')} to {moh_df.end_datetime[moh_df.start_datetime < dt.datetime(2262,1,1,0,0,0)].max().strftime('%d/%m/%Y %H:%M')}")  
    bad_datetimes = moh_df[moh_df.start_datetime >= moh_df.end_datetime]
    no_bad_datetimes = bad_datetimes.shape[0]
    unknown_datetimes = moh_df[moh_df.start_datetime == dt.datetime(2262,1,1,0,0,0)]
    no_unknown_datetimes = unknown_datetimes.shape[0]
    print(f"Bad start/end datetimes: {no_bad_datetimes}") 
    print(f"Unknown datetimes: {no_unknown_datetimes}") 
    print(f"Total valid datapoints: {moh_df.shape[0] - no_bad_datetimes - no_unknown_datetimes}")
    #Enable these for debug prints of df.
    #print("Subset of valid datapoints:")
    #display(moh_df) 
    #print("Subset of bad start/end datapoints:")
    #display(bad_datetimes)
    #print("Subset of unknown datetime datapoints:")
    #display(unknown_datetimes)
    return moh_df


def bigUglyCrosscheckloops(moh_df, loch_slice):
    TIME_BUFFER = dt.timedelta(minutes=0)
    # Counters for bad datapoints
    # Note: These were counters, now they are lists.
    counters = { 'unknown_datetime' : [], 'bad_datetime' : [], 'missing_results' : [], 'debug_timestamp' : [], 'totally_failed' : []}
    # Total dataset size
    frame_size = moh_df.shape[0]

    # Do I really need to create this DataFrame? No.
    results_df = pd.DataFrame(columns=['incident_id', 'incident_time', 'incident_location', 'incident_name','incident_place', 'incident_comments', 'min_distance_location','min_distance_accuracy', 'min_distance_distance', 'min_accuracy_accuracy'])
    for inc_idx, inc_row in moh_df.iterrows():
        # Everything is in a try, wham? because MOD data can lie!
        try:
            # Time buffers! may be useful in the future
            search_start_datetime = inc_row.start_datetime - TIME_BUFFER
            search_end_datetime = inc_row.end_datetime + TIME_BUFFER
            
            # Some sanity checks
            if inc_row.start_datetime > inc_row.end_datetime:
                counters['bad_datetime'].append(inc_row)
                continue
            if inc_row.start_datetime == dt.datetime(2262,1,1,0,0,0):
                counters['unknown_datetime'].append(inc_row)
                continue
                # Select the whole timerange
                search_start_datetime = dt.datetime.combine(search_start_datetime.date(), dt.time(0,0))
                search_end_datetime = dt.datetime.combine(search_end_datetime.date() + dt.timedelta(days=1), dt.time(0,0))
            # Debug, just to see how many timestams are *maybe* correct
            if inc_row.debug_fromTime != inc_row.debug_toTime:
                counters['debug_timestamp'].append(inc_row)

            # Get all locations from location history that correspond to the datetime
            loch_results = loch_slice[(loch_slice.datetime >= search_start_datetime ) & (loch_slice.datetime <= search_end_datetime)]
            if loch_results.empty:
                # No results, manual verification :(
                counters['missing_results'].append(inc_row)
                continue

            # We can only set a distance and index, but it's easy to run slicings on a clean DataFrame
            measured_distances = pd.DataFrame(columns=['index','datetime', 'distance', 'latitude', 'longitude', 'accuracy'])
            for loc_idx, loc_row in loch_results.iterrows():
                distance = geopy.distance.geodesic((inc_row.longitude, inc_row.latitude),(loc_row.longitude, loc_row.latitude))
                measured_distances = measured_distances.append({ 'index' : loc_idx,
                                                            'datetime' : loc_row.datetime,
                                                            'distance' : distance.km,
                                                            'latitude' : loc_row.latitude,
                                                            'longitude' : loc_row.longitude,
                                                            'accuracy' : loc_row.accuracy}, ignore_index=True)
            
            min_distance_df = measured_distances.sort_values(by=['distance'], ).head(1)
            min_accuracy_df = measured_distances.sort_values(by=['accuracy'], ).head(1)
            
            results_df = results_df.append({'incident_id' : inc_row.id, 
                                    'incident_time' : str(f'{inc_row.start_datetime.strftime("%d/%m/%Y %H:%M")} - {inc_row.end_datetime.strftime("%d/%m/%Y %H:%M")}'), 
                                    'incident_location' : (inc_row.latitude, inc_row.longitude), 
                                    'incident_name' : inc_row.iname,
                                    'incident_place' : inc_row.place, 
                                    'incident_comments' : inc_row.comments,
                                    # I don't know why (yet) but I needed to use here .item()
                                    'min_distance_location' : (min_distance_df.latitude.item(), min_distance_df.longitude.item()), 
                                    'min_distance_accuracy' : min_distance_df.accuracy.item(),
                                    'min_distance_distance' : min_distance_df.distance.item(),
                                    'min_accuracy_location' : (min_accuracy_df.latitude.item(), min_accuracy_df.longitude.item()), 
                                    'min_accuracy_distance' : min_accuracy_df.distance.item(),
                                    'min_accuracy_accuracy' : min_accuracy_df.accuracy.item()}, ignore_index=True)
        except Exception as ex:
            # Yep, the data lied.
            print(f"You have 1 new exception: {ex}")
            counters['totally_failed'].append(inc_row)
        
        # A poor mans progress meter.
        if inc_idx % 100 == 0:
            print(f"\rProcessed {inc_idx}/{frame_size} exposure incidents (estimate)", end='')
    print(" Done")
    return results_df, counters




def main():
    startTime = dt.datetime.now()
    if (len(sys.argv) != 2) or (not os.path.isfile(sys.argv[1])):
        print(f"Usage: {__file__} <location_history_file>")
        return 1

    print("Downloading latest MOH data")
    downloadFile(GOV_CASE_LOC_URL, 'govData.json')
    print()
    print("Loading Google location history")
    loch_df = transformLocationHistory(sys.argv[1])
    print()
    print("Loading MOH exposure incidents data")
    moh_df = transformMOHData("govData.json")
    print()
    print("Selecting relevent slice from location history.")
    loch_slice = loch_df[(loch_df.datetime >=  moh_df.start_datetime.min()) & (loch_df.datetime <= moh_df.end_datetime[moh_df.end_datetime < dt.datetime(2262,1,1,0,0,0)].max())]
    print(f"Location history slice from {loch_slice.datetime.min().strftime('%d/%m/%Y %H:%M')} to {loch_slice.datetime.max().strftime('%d/%m/%Y %H:%M')}")
    print()
    results, counters = bigUglyCrosscheckloops(moh_df, loch_slice)
    results_text = f"Total incidents checked: {results.shape[0]}\n"
    results_text += f"Incidents with unknown time skipped: {len(counters['unknown_datetime'])}\n"
    results_text += f"Incidents with bad time skipped: {len(counters['bad_datetime'])}\n"
    results_text += f"Incidents with missing results: {len(counters['missing_results'])}\n"
    results_text += f"Incidents witch just failed for some reason: {len(counters['totally_failed'])}\n"
    results_text += f"Debug timestamp incidents: {len(counters['debug_timestamp'])}\n"
    print(results_text)
    print("Saving to file...")
    with open(dt.datetime.now().strftime("results%Y%m%d_%H%M.html"), 'w', encoding="utf8") as out:
        results = results.sort_values(by=['min_distance_distance'])
        # These dataframes are totally unnecessary (and inefficient), but I cant be bothred to change bigUglyCrosscheckloops
        # as all I want to do is to use the .to_html functionality.
        missing_results = pd.DataFrame(counters['missing_results'])
        unknown_datetime_results = pd.DataFrame(counters['unknown_datetime'])
        bad_times_results = pd.DataFrame(counters['bad_datetime'])
        html_text = results_text.replace("\n", "<br/>")
        #pdb.set_trace()
        out.write(f"""<html><head><meta charset="UTF-8"></head><body>
            <h1>Location history cross referencing for {dt.datetime.now().strftime("%m/%d/%Y %H:%M")}</h1>{html_text}<br/>Tip: You can copy and paste any "_location" value into google maps.
            <h1 style="color:#ff0000;">Locations found, distance < 1km ({results[results['min_distance_distance'] < 1].shape[0]} results)</h1><div dir="rtl">{results[results['min_distance_distance'] < 1].to_html()}</div><br/>
            <h1 style="color:#ffa500;">Locations missing from history ({missing_results.shape[0]} results) </h1><div dir="rtl">{missing_results.to_html()}</div><br/>
            <h1 style="color:#ebe939;">Locations with bad time information ({bad_times_results.shape[0]} results)</h1><div dir="rtl">{bad_times_results.to_html()}</div><br/>
            <h1 style="color:#ebe939;">Locations with unknown time information ({unknown_datetime_results.shape[0]} results)</h1><div dir="rtl">{unknown_datetime_results.to_html()}</div><br/>
            <h1>All locations found (order: distance ascending)</h1><div dir="rtl">{results.to_html()}</div>
            </body></html>""")

    print(dt.datetime.now() - startTime)
    return 0
    

    


if __name__ == "__main__":
    sys.exit(main())

