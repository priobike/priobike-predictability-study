import json
import math
import os
from collections import defaultdict

import pandas as pd


def fetch_city_map(min_lat, min_lng, max_lat, max_lng, style="satellite-streets-v12", aspect=1.0):
    """
    Fetch a PNG static image from the Mapbox API and plot it.

    Token is from: https://docs.mapbox.com/api/maps/static-images/
    """
    from tempfile import TemporaryDirectory

    import matplotlib.image as mpimg
    import requests

    width = min(1000, int(1000 * aspect))
    height = 1000

    # Get the image from the Mapbox API
    url = f"https://api.mapbox.com/styles/v1/mapbox/{style}/static/[{min_lng},{min_lat},{max_lng},{max_lat}]/{height}x{width}?" \
        + f"padding=0" \
        + f"&@2x" \
        + f"&access_token=pk.eyJ1Ijoic25ybXR0aHMiLCJhIjoiY2w0ZWVlcWt5MDAwZjNjbW5nMHNvN3kwNiJ9.upoSvMqKIFe3V_zPt1KxmA"
    img_data = requests.get(url).content

    # Save the image to a temporary file
    with TemporaryDirectory() as f:
        f = f + "/map.jpeg"
        with open(f, "wb") as handler:
            handler.write(img_data)

        # Read the image from the temporary file
        img = mpimg.imread(f)
        
    return img

def read_preprocessed_tracks(data_dir):
    """
    Read all preprocessed tracks from the data directory as pandas dataframes.
    """

    dtype = {
        'timestamp': 'float64',
        'longitude': 'float64',
        'latitude': 'float64',
        'speed': 'float64',
        'accuracy': 'float64',
        'acc_x': 'float64',
        'acc_y': 'float64',
        'acc_z': 'float64',
        'mag_x': 'float64',
        'mag_y': 'float64',
        'mag_z': 'float64',
        'gyr_x': 'float64',
        'gyr_y': 'float64',
        'gyr_z': 'float64',
        'snapped_lat': 'float64',
        'snapped_lng': 'float64',
        'route_dist': 'float64',
        'sg_dist': 'float64',
        'sg': 'string',
        'prediction_quality': 'float64',
        # json object
        'recommendation': 'string',
    }

    track_files = [f for f in os.listdir(f'{data_dir}/preprocessed') if f.endswith('.csv')]
    track_metadata_files = [f for f in os.listdir(f'{data_dir}/preprocessed') if f.endswith('.json')]
    start_times = []
    for track_file in track_files:
        track = pd.read_csv(f'{data_dir}/preprocessed/{track_file}', dtype=dtype)
        start_time = int(track['timestamp'].iloc[0])
        start_times.append(start_time)
    
    # Sort the tracks by start time
    track_files = [f for _, f in sorted(zip(start_times, track_files), key=lambda pair: pair[0])]
    track_metadata_files = [f for _, f in sorted(zip(start_times, track_metadata_files), key=lambda pair: pair[0])]
    for track_file, track_metadata_file in zip(track_files, track_metadata_files):
        track = pd.read_csv(f'{data_dir}/preprocessed/{track_file}', dtype=dtype)
        with open(f'{data_dir}/preprocessed/{track_metadata_file}') as f:
            track_metadata = json.load(f)
        yield track, track_metadata

def read_raw_tracks(data_dir):
    """
    Read all tracks from the data directory as a generator, together with feedback from users.

    The tracks will be sorted by their start time.
    """
    track_files = [f for f in os.listdir(f'{data_dir}/tracks') if f.endswith('.json')]
    start_times = []
    for track_file in track_files:
        with open(f'{data_dir}/tracks/{track_file}') as f:
            track = json.load(f)
        start_time = track['metadata']['startTime']
        start_times.append(start_time)

    # Prepopulate the answers
    answer_files = [f for f in os.listdir(f'{data_dir}/answers') if f.endswith('.json')]
    answers_by_session_id = defaultdict(list)
    for answer_file in answer_files:
        with open(f'{data_dir}/answers/{answer_file}') as f:
            answer = json.load(f)
        session_id = answer['sessionId']
        answers_by_session_id[session_id].append(answer)

    # Sort the tracks by start time
    track_files = [f for _, f in sorted(zip(start_times, track_files), key=lambda pair: pair[0])]
    for track_file in track_files:
        with open(f'{data_dir}/tracks/{track_file}') as f:
            track = json.load(f)
        session_id = track['metadata']['sessionId']
        track['answers'] = answers_by_session_id[session_id]
        yield track

def smooth(scalars, weight):  # Weight between 0 and 1
    """
    Smooth the input scalars using the exponential moving average.
    """
    last = scalars[0]  # First value in the plot (first timestep)
    smoothed = list()
    for point in scalars:
        smoothed_val = last * weight + (1 - weight) * point  # Calculate smoothed value
        smoothed.append(smoothed_val)                        # Save it
        last = smoothed_val                                  # Anchor the last smoothed value
        
    return smoothed

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees) in meters.
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371000  # Radius of earth in meters. Use 3956 for miles
    return c * r

def snap(pos_lat, pos_lon, p1_lat, p1_lon, p2_lat, p2_lon):
    """
    Calculate the nearest point on the line between p1 and p2,
    with respect to the reference point pos.
    """
    x = pos_lat
    y = pos_lon
    x1 = p1_lat
    y1 = p1_lon
    x2 = p2_lat
    y2 = p2_lon

    A = x - x1
    B = y - y1
    C = x2 - x1
    D = y2 - y1

    dot = A * C + B * D
    lenSq = C * C + D * D
    param = -1.0
    if lenSq != 0:
        param = dot / lenSq

    if param < 0:
        # Snap to point 1.
        xx = x1
        yy = y1
    elif param > 1:
        # Snap to point 2.
        xx = x2
        yy = y2
    else:
        # Snap to shortest point inbetween.
        xx = x1 + param * C
        yy = y1 + param * D
    return xx, yy
