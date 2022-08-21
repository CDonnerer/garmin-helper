"""Savely convert a FIT file to GPX

This will protect against corrupted records in FIT files that prevent uploads.
"""

import argparse
import datetime
import logging
import sys
from dataclasses import dataclass

import fitparse
import gpxpy.gpx
import pandas as pd

_logger = logging.getLogger(__name__)


@dataclass
class Point:
    cadence: int
    distance: float
    enhanced_speed: float
    heart_rate: int
    position_lat: int
    position_long: int
    speed: float
    timestamp: datetime.datetime
    unknown_88: int


def read_fit_file(fit_file):
    fitfile = fitparse.FitFile(fit_file)
    points = list()
    records = fitfile.get_messages("record")

    while True:
        try:
            record = next(records)
            points.append(Point(**record.get_values()))
        except StopIteration as e:
            _logger.info("Read all records in FIT file.")
            break
        except Exception as e:
            _logger.info("Error in record, continuing")
    return points


def semicircle_to_degrees(val):
    return val / ((2**32) / 360)


def points_to_pdf(points):
    df = pd.DataFrame(points)

    # Capping of outliers - may be needed
    # outliers = df_raw["distance"] > 53553.34
    # df = df_raw.loc[~outliers, :]

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["latitude"] = semicircle_to_degrees(df["position_lat"])
    df["longitude"] = semicircle_to_degrees(df["position_long"])
    return df


def pdf_to_gpx(df):
    gpx = gpxpy.gpx.GPX()

    # Create first track in our GPX:
    gpx_track = gpxpy.gpx.GPXTrack()
    gpx.tracks.append(gpx_track)

    # Create first segment in our GPX track:
    gpx_segment = gpxpy.gpx.GPXTrackSegment()
    gpx_track.segments.append(gpx_segment)

    # Add points from dataframe to GPX track:
    df = df.copy().dropna(axis=0, subset=["latitude", "longitude", "timestamp"])

    for _, row in df.iterrows():
        track_point = gpxpy.gpx.GPXTrackPoint(
            latitude=row["latitude"],
            longitude=row["longitude"],
            time=pd.Timestamp(row["timestamp"]),
        )
        gpx_segment.points.append(track_point)
    return gpx


def setup_logging(loglevel):
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


def parse_args(args):
    parser = argparse.ArgumentParser(description="Add me")
    parser.add_argument(
        "-ff",
        "--fit",
        dest="fit",
        help="Path to input FIT file",
    )
    parser.add_argument(
        "-g",
        "--gpx",
        dest="gpx",
        help="Path to output GPX file",
    )
    return parser.parse_args(args)


def main(args):
    args = parse_args(args)
    setup_logging(logging.INFO)

    points = read_fit_file(args.fit)
    df = points_to_pdf(points)
    gpx = pdf_to_gpx(df)

    with open(args.gpx, "w") as f:
        f.write(gpx.to_xml())


if __name__ == "__main__":
    main(sys.argv[1:])
