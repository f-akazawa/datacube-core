from __future__ import absolute_import

import boto3
import pytest


def test_1():
    s3 = boto3.resource('s3')
    objects = s3.meta.client.list_objects(Bucket='travis-testing')
    print(objects)
