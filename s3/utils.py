"""General utilities for S3 buckets"""

from __future__ import annotations

import copy
import logging
import os
import shutil

import boto3
import geopandas as gpd
from pyproj.crs import CRS
import rasterio

# from botocore.errorfactory import ClientError


# def assert_file_exists(file: str):
#     """Asserts existence of file path provided. Handles local or S3 paths."""
#     common_exception = RuntimeError(f"file not found: {file}")
#     if file.startswith("s3://"):
#         bucket, key = extract_bucketname_and_keyname(s3path=file)
#         s3 = get_sessioned_s3_resource()
#         try:
#             s3.meta.client.head_object(Bucket=bucket, Key=key)
#         except ClientError as e:
#             raise common_exception from e
#     else:
#         if not os.path.isfile(file):
#             raise common_exception


def upload_file_to_s3__or__copy_local_file(
    src_file: str,
    tgt_file: str,
) -> None:
    logging.info(f"Copying to: {tgt_file}")
    if tgt_file.startswith("s3://"):
        bucket, key = extract_bucketname_and_keyname(s3path=tgt_file)
        s3 = get_sessioned_s3_resource()
        s3.meta.client.upload_file(Filename=src_file, Bucket=bucket, Key=key)
    else:
        os.makedirs(os.path.dirname(tgt_file), exist_ok=True)
        shutil.copy2(src_file, tgt_file)


def download_file_from_s3(src_file: str, tgt_file: str) -> None:
    logging.info(f"Downloading file: {src_file} -> {tgt_file}")
    bucket, key = extract_bucketname_and_keyname(s3path=src_file)
    s3 = get_sessioned_s3_resource()
    s3.meta.client.head_object(Bucket=bucket, Key=key)
    os.makedirs(os.path.dirname(tgt_file), exist_ok=True)
    s3.meta.client.download_file(Bucket=bucket, Key=key, Filename=tgt_file)


def delete_file_if_exists(file: str):
    logging.info(f"Deleting if exists: {file}")
    if file.startswith("s3://"):
        bucket, key = extract_bucketname_and_keyname(s3path=file)
        s3 = get_sessioned_s3_resource()
        s3.meta.client.delete_object(Bucket=bucket, Key=key)
    else:
        try:
            os.remove(file)
        except FileNotFoundError:
            pass


def extract_bucketname_and_keyname(s3path: str) -> tuple[str, str]:
    if not s3path.startswith("s3://"):
        raise ValueError(f"s3path does not start with s3://: {s3path}")
    bucket, _, key = s3path[5:].partition("/")
    return bucket, key


def get_sessioned_s3_resource() -> boto3.resources.factory.s3.ServiceResource:
    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
    )
    s3 = session.resource("s3")
    return s3


def get_crs_of_raster_file(path: str) -> CRS:
    with rasterio.Env():
        with rasterio.open(path) as rast:
            crs = CRS.from_user_input(rast.crs)
    return crs


def get_crs_of_vector_file(path: str) -> CRS:
    gdf = gpd.read_file(path)
    return copy.deepcopy(gdf.crs)
