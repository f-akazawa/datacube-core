# coding=utf-8
"""
Tables for indexing the datasets which were ingested into the AGDC.
"""
from __future__ import absolute_import

import logging

from sqlalchemy import ForeignKey, UniqueConstraint, PrimaryKeyConstraint, CheckConstraint, SmallInteger
from sqlalchemy import Table, Column, Integer, String, DateTime, Boolean
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.sql import func

from . import _core, _sql

_LOG = logging.getLogger(__name__)

METADATA_TYPE = Table(
    'metadata_type', _core.METADATA,
    Column('id', SmallInteger, primary_key=True, autoincrement=True),

    Column('name', String, unique=True, nullable=False),

    Column('definition', postgres.JSONB, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', _sql.PGNAME, server_default=func.current_user(), nullable=False),

    # Name must be alphanumeric + underscores.
    CheckConstraint(r"name ~* '^\w+$'", name='alphanumeric_name'),
)

DATASET_TYPE = Table(
    'dataset_type', _core.METADATA,
    Column('id', SmallInteger, primary_key=True, autoincrement=True),

    # A name/label for this type (eg. 'ls7_nbar'). Specified by users.
    Column('name', String, unique=True, nullable=False),

    # All datasets of this type should contain these fields.
    # (newly-ingested datasets may be matched against these fields to determine the dataset type)
    Column('metadata', postgres.JSONB, nullable=False),

    # The metadata format expected (eg. what fields to search by)
    Column('metadata_type_ref', None, ForeignKey(METADATA_TYPE.c.id), nullable=False),

    Column('definition', postgres.JSONB, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', _sql.PGNAME, server_default=func.current_user(), nullable=False),

    # Name must be alphanumeric + underscores.
    CheckConstraint(r"name ~* '^\w+$'", name='alphanumeric_name'),
)

DATASET = Table(
    'dataset', _core.METADATA,
    Column('id', postgres.UUID(as_uuid=True), primary_key=True),

    Column('metadata_type_ref', None, ForeignKey(METADATA_TYPE.c.id), nullable=False),
    Column('dataset_type_ref', None, ForeignKey(DATASET_TYPE.c.id), index=True, nullable=False),

    Column('metadata', postgres.JSONB, index=False, nullable=False),

    # Date it was archived. Null for active datasets.
    Column('archived', DateTime(timezone=True), default=None, nullable=True),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', _sql.PGNAME, server_default=func.current_user(), nullable=False),
)


# Required to store:
#     dataset_ref
#     dataset key (different from dataset_ref) given by s3_hash.py
#         contingency plan.
#     attribute (bands)
#         must match dataset.metadata::image::bands
#         required because we are putting 1 band per s3 object. NetCDF contains all bands.
#     macro shape of s3 objects e.g (2, 2)
#     shape e.g. (20, 1024, 1024)
#     numpy dtype e.g. (int16)
#     regular index (min, max, step)
#         spatial
#         temporal
#     irregular index (enumerated list)
#         spatial
#         temporal
#     per s3 object
#         bucket (s3 bucket)
#         key (s3 key) given by s3_hash.py
#         compression scheme - None
#         shape: the shape of the parent block
#         micro shape e.g. (20, 512, 512)
#         chunksize? leave for later
#         dimensions - 
#         valid times[]
#         
#         regular index - min, max and step size for each dim
#             spatial
#             temporal
#         irregular index 
#             spatial
#             temporal
#         Array of contained timestamps

S3_SUB_DATASET = Table(
    "s3_sub_dataset", _core.METADATA,
    # Object 
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('bucket', String, nullable=False),
    Column('key', String, nullable=False),
    Column('compression_scheme', String, nullable=False),
    Column('shape', _sql.ARRAY(Integer), nullable=False),
    Column('geo_extents', _sql.ARRAY(Integer), nullable=False)
)

DATASET_LOCATION = Table(
    'dataset_location', _core.METADATA,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('dataset_ref', None, ForeignKey(DATASET.c.id), index=True, nullable=False),

    # The base URI to find the dataset.
    #
    # All paths in the dataset metadata can be computed relative to this.
    # (it is often the path of the source metadata file)
    #
    # eg 'file:///g/data/datasets/LS8_NBAR/agdc-metadata.yaml' or 'ftp://eo.something.com/dataset'
    # 'file' is a scheme, '///g/data/datasets/LS8_NBAR/agdc-metadata.yaml' is a body.
    Column('uri_scheme', String, nullable=False),
    Column('uri_body', String, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', _sql.PGNAME, server_default=func.current_user(), nullable=False),

    UniqueConstraint('uri_scheme', 'uri_body', 'dataset_ref'),
)

# Link datasets to their source datasets.
DATASET_SOURCE = Table(
    'dataset_source', _core.METADATA,
    Column('dataset_ref', None, ForeignKey(DATASET.c.id), nullable=False),

    # An identifier for this source dataset.
    #    -> Usually it's the dataset type ('ortho', 'nbar'...), as there's typically only one source
    #       of each type.
    Column('classifier', String, nullable=False),
    Column('source_dataset_ref', None, ForeignKey(DATASET.c.id), nullable=False),

    PrimaryKeyConstraint('dataset_ref', 'classifier'),
    UniqueConstraint('source_dataset_ref', 'dataset_ref'),
)
