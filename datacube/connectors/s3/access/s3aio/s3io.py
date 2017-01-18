'''
S3IO Class

Low level byte read/writes to S3

Single-threaded. set new_session = False
Multi-threaded. set new_session = True

'''

import io
import os
import time
import boto3
import botocore
import numpy as np
from itertools import repeat
from multiprocessing import Pool, freeze_support, cpu_count
import SharedArray as sa
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# pylint: disable=too-many-locals


class S3IO:

    def __init__(self):
        self.dfd = None

    def list_created_arrays(self):
        result = [f for f in os.listdir("/dev/shm") if f.startswith('S3IO')]
        print(result)
        return result

    def delete_created_arrays(self):
        for a in self.list_created_arrays():
            sa.delete(a)

    def list_buckets(self, new_session=False):
        try:
            if new_session is True:
                s3 = boto3.session.Session().resource('s3')
            else:
                s3 = boto3.resource('s3')

            return [b['Name'] for b in s3.meta.client.list_buckets()['Buckets']]
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            raise Exception("ClientError", error_code)

    def list_objects(self, bucket, prefix='', max_keys=100, new_session=False):
        try:
            if new_session is True:
                s3 = boto3.session.Session().resource('s3')
            else:
                s3 = boto3.resource('s3')

            objects = s3.meta.client.list_objects(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
            if 'Contents' not in objects:
                return []
            return [o['Key'] for o in
                    s3.meta.client.list_objects(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)['Contents']]
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            raise Exception("ClientError", error_code)

    def bucket_exists(self, s3_bucket, new_session=False):
        try:
            if new_session is True:
                s3 = boto3.session.Session().resource('s3')
            else:
                s3 = boto3.resource('s3')
            s3.meta.client.head_bucket(Bucket=s3_bucket)
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                return False
            raise Exception("ClientError", error_code)
        return True

    def object_exists(self, s3_bucket, s3_object, new_session=False):
        try:
            if new_session is True:
                s3 = boto3.session.Session().resource('s3')
            else:
                s3 = boto3.resource('s3')
            s3.meta.client.head_object(Bucket=s3_bucket, Key=s3_object)
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                return False
            raise Exception("ClientError", error_code)
        return True

    def put_bytes(self, s3_bucket, s3_object, data, new_session=False):
        # assert isinstance(data, memoryview), 'data must be a memoryview'
        if new_session is True:
            s3 = boto3.session.Session().resource('s3')
        else:
            s3 = boto3.resource('s3')
        s3.meta.client.put_object(Bucket=s3_bucket, Key=s3_object, Body=io.BytesIO(data))

    # # functionality for byte range put does not exist in S3 API
    # # need to do a get, change the bytes in the byte range and upload_part
    # # do later
    # def put_byte_range(self, s3_bucket, s3_object, data, s3_start, s3_end, new_session=False):
    #     if new_session is True:
    #         s3 = boto3.session.Session().resource('s3')
    #     else:
    #         s3 = boto3.resource('s3')
    #     s3.meta.client.put_object(Bucket=s3_bucket, Key=s3_object, Range='bytes='+str(s3_start)+'-'+str(s3_end-1),
    #                               Body=io.BytesIO(data))

    def put_bytes_mpu(self, s3_bucket, s3_object, data, block_size, new_session=False):
        assert isinstance(data, memoryview), 'data must be a memoryview'
        if new_session is True:
            s3 = boto3.session.Session().resource('s3')
        else:
            s3 = boto3.resource('s3')
        mpu = s3.meta.client.create_multipart_upload(Bucket=s3_bucket, Key=s3_object)

        num_blocks = int(np.ceil(data.nbytes/float(block_size)))
        parts_dict = dict(Parts=[])

        for block_number in range(num_blocks):
            part_number = block_number + 1
            start = block_number*block_size
            end = (block_number+1)*block_size
            if end > data.nbytes:
                end = data.nbytes
            data_chunk = io.BytesIO(data[start:end])

            response = s3.meta.client.upload_part(Bucket=s3_bucket,
                                                  Key=s3_object,
                                                  UploadId=mpu['UploadId'],
                                                  PartNumber=part_number,
                                                  Body=data_chunk)

            parts_dict['Parts'].append(dict(PartNumber=part_number, ETag=response['ETag']))

        mpu_response = s3.meta.client.complete_multipart_upload(Bucket=s3_bucket,
                                                                Key=s3_object,
                                                                UploadId=mpu['UploadId'],
                                                                MultipartUpload=parts_dict)
        return mpu_response

    def work_put(self, args):
        return self.work_put_impl(*args)

    def work_put_impl(self, block_number, data, s3_bucket, s3_object, block_size, mpu):
        response = boto3.resource('s3').meta.client.upload_part(Bucket=s3_bucket,
                                                                Key=s3_object,
                                                                UploadId=mpu['UploadId'],
                                                                PartNumber=block_number + 1,
                                                                Body=data)

        return dict(PartNumber=block_number + 1, ETag=response['ETag'])

    def put_bytes_mpu_mp(self, s3_bucket, s3_object, data, block_size, new_session=False):
        if new_session is True:
            s3 = boto3.session.Session().resource('s3')
        else:
            s3 = boto3.resource('s3')

        mpu = s3.meta.client.create_multipart_upload(Bucket=s3_bucket, Key=s3_object)
        num_blocks = int(np.ceil(data.nbytes/float(block_size)))
        data_chunks = []
        for block_number in range(num_blocks):
            start = block_number*block_size
            end = (block_number+1)*block_size
            if end > data.nbytes:
                end = data.nbytes
            data_chunks.append(io.BytesIO(data[start:end]))

        parts_dict = dict(Parts=[])
        blocks = range(num_blocks)
        num_processes = cpu_count()
        pool = Pool(num_processes)
        results = pool.map_async(self.work_put, zip(blocks, data_chunks, repeat(s3_bucket), repeat(s3_object),
                                                    repeat(block_size), repeat(mpu)))
        pool.close()
        pool.join()

        for result in results.get():
            parts_dict['Parts'].append(result)

        mpu_response = boto3.resource('s3').meta.client.complete_multipart_upload(Bucket=s3_bucket,
                                                                                  Key=s3_object,
                                                                                  UploadId=mpu['UploadId'],
                                                                                  MultipartUpload=parts_dict)

        return mpu_response

    def work_put_shm(self, args):
        return self.work_put_impl_shm(*args)

    def work_put_impl_shm(self, block_number, array_name, s3_bucket, s3_object, block_size, mpu):
        part_number = block_number + 1
        start = block_number*block_size
        end = (block_number+1)*block_size
        shared_array = sa.attach(array_name)
        data_chunk = io.BytesIO(shared_array.data[start:end])

        s3 = boto3.session.Session().resource('s3')
        # s3 = boto3.resource('s3')
        response = s3.meta.client.upload_part(Bucket=s3_bucket,
                                              Key=s3_object,
                                              UploadId=mpu['UploadId'],
                                              PartNumber=part_number,
                                              Body=data_chunk)

        return dict(PartNumber=part_number, ETag=response['ETag'])

    def put_bytes_mpu_mp_shm(self, s3_bucket, s3_object, array_name, block_size, new_session=False):
        if new_session is True:
            s3 = boto3.session.Session().resource('s3')
        else:
            s3 = boto3.resource('s3')

        mpu = s3.meta.client.create_multipart_upload(Bucket=s3_bucket, Key=s3_object)

        shared_array = sa.attach(array_name)
        num_blocks = int(np.ceil(shared_array.nbytes/float(block_size)))
        parts_dict = dict(Parts=[])
        blocks = range(num_blocks)
        num_processes = cpu_count()
        pool = Pool(num_processes)
        results = pool.map_async(self.work_put_shm, zip(blocks, repeat(array_name), repeat(s3_bucket),
                                                        repeat(s3_object), repeat(block_size), repeat(mpu)))
        pool.close()
        pool.join()

        for result in results.get():
            parts_dict['Parts'].append(result)

        mpu_response = s3.meta.client.complete_multipart_upload(Bucket=s3_bucket,
                                                                Key=s3_object,
                                                                UploadId=mpu['UploadId'],
                                                                MultipartUpload=parts_dict)
        return mpu_response

    def get_bytes(self, s3_bucket, s3_object, new_session=False):
        while True:
            if new_session is True:
                s3 = boto3.session.Session().resource('s3')
            else:
                s3 = boto3.resource('s3')
            b = s3.Bucket(s3_bucket)
            o = b.Object(s3_object)
            try:
                d = o.get()['Body'].read()
                # d = np.frombuffer(d, dtype=np.uint8, count=-1, offset=0)
                return d
            except botocore.exceptions.ClientError as e:
                break
            break

    def get_byte_range(self, s3_bucket, s3_object, s3_start, s3_end, new_session=False):
        while True:
            if new_session is True:
                s3 = boto3.session.Session().resource('s3')
            else:
                s3 = boto3.resource('s3')
            b = s3.Bucket(s3_bucket)
            o = b.Object(s3_object)
            try:
                d = o.get(Range='bytes='+str(s3_start)+'-'+str(s3_end-1))['Body'].read()
                d = np.frombuffer(d, dtype=np.uint8, count=-1, offset=0)
                return d
            except botocore.exceptions.ClientError as e:
                break
            break

    def work_get(self, args):
        return self.work_get_impl(*args)

    def work_get_impl(self, block_number, array_name, s3_bucket, s3_object, s3_max_size, block_size):
        start = block_number*block_size
        end = (block_number+1)*block_size
        if end > s3_max_size:
            end = s3_max_size
        d = self.get_byte_range(s3_bucket, s3_object, start, end, True)
        # d = np.frombuffer(d, dtype=np.uint8, count=-1, offset=0)
        shared_array = sa.attach(array_name)
        shared_array[start:end] = d

    def get_byte_range_mp(self, s3_bucket, s3_object, s3_start, s3_end, block_size, new_session=False):
        if new_session is True:
            s3 = boto3.session.Session().resource('s3')
        else:
            s3 = boto3.resource('s3')

        s3o = s3.Bucket(s3_bucket).Object(s3_object).get()
        s3_max_size = s3o['ContentLength']
        s3_obj_size = s3_end-s3_start
        num_streams = int(np.ceil(s3_obj_size/block_size))
        num_processes = cpu_count()
        pool = Pool(num_processes)
        blocks = range(num_streams)
        array_name = '_'.join(['S3IO', s3_bucket, s3_object, str(os.getpid())])
        sa.create(array_name, shape=(s3_obj_size), dtype=np.uint8)
        shared_array = sa.attach(array_name)

        pool.map_async(self.work_get, zip(blocks, repeat(array_name), repeat(s3_bucket), repeat(s3_object),
                                          repeat(s3_max_size), repeat(block_size)))
        pool.close()
        pool.join()
        sa.delete(array_name)
        return shared_array
