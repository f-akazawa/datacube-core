'''
S3AIO Class

Array wrapper class

'''

import itertools
import numpy as np
from .s3io import S3IO


class S3AIO(object):

    def __init__(self):
        self.s3io = S3IO()

    def bytes_to_array(self, data, shape, dtype):
        array = np.empty(shape=shape, dtype=dtype)
        array.data[0:len(data)] = data
        return array

    def copy_bytes_to_shared_array(self, shared_array, start, end, data):
        shared_array.data[start:end] = data

    def chunks_indices_1d(self, begin, end, step):
        for i in range(begin, end, step):
            yield slice(i, min(end, i + step))

    def chunk_indices_nd(self, shape, chunk):
        var1 = map(self.chunks_indices_1d, itertools.repeat(0), shape, chunk)
        return itertools.product(*var1)

    def shard_array_to_s3(self, array, indices, s3_bucket, s3_keys):
        for s3_key, index in zip(s3_keys, indices):
            self.s3io.put_bytes(s3_bucket, s3_key, bytes(array[index].data))

    # @profile
    def assemble_array_from_s3(self, array, indices, s3_bucket, s3_keys, dtype):
        for s3_key, index in zip(s3_keys, indices):
            b = self.s3io.get_bytes(s3_bucket, s3_key)
            m = memoryview(b)
            shape = tuple((i.stop - i.start) for i in index)
            array[index] = np.ndarray(shape, buffer=m, dtype=dtype)
        return array

    def to_1d(self, index, shape):
        return np.ravel_multi_index(index, shape)

    def to_nd(self, index, shape):
        np.unravel_index(index, shape)

    def get_point(self, index_point, shape, dtype, s3_bucket, s3_key):
        item_size = np.dtype(dtype).itemsize
        idx = self.to_1d(index_point, shape) * item_size
        b = self.s3io.get_byte_range(s3_bucket, s3_key, idx, idx+item_size)
        a = np.frombuffer(b, dtype=dtype, count=-1, offset=0)
        return a

    def cdims(self, slices, shape):
        return [sl.start == 0 and sl.stop == sh and (sl.step is None or sl.step == 1)
                for sl, sh in zip(slices, shape)]

    def get_slice(self, array_slice, shape, dtype, s3_bucket, s3_key):  # pylint: disable=too-many-locals
        # convert array_slice into into sub-slices of maximum contiguous blocks

        # Todo:
        #   - parallelise reads and writes
        #     - 1. get memory rows in parallel and merge
        #     - 2. get bounding block in parallel and subset.

        cdim = self.cdims(array_slice, shape)

        try:
            end = cdim[::-1].index(False)+1
        except ValueError:
            end = len(shape)

        start = len(shape) - end

        outer = array_slice[:-end]
        outer_ranges = [range(s.start, s.stop) for s in outer]
        outer_cells = list(itertools.product(*outer_ranges))
        blocks = list(zip(outer_cells, itertools.repeat(array_slice[start:])))
        item_size = np.dtype(dtype).itemsize

        results = []
        for cell, sub_range in blocks:
            # print(cell, sub_range)
            s3_start = (np.ravel_multi_index(cell+tuple([s.start for s in sub_range]), shape)) * item_size
            s3_end = (np.ravel_multi_index(cell+tuple([s.stop-1 for s in sub_range]), shape)+1) * item_size
            # print(s3_start, s3_end)
            data = self.s3io.get_byte_range(s3_bucket, s3_key, s3_start, s3_end)
            results.append((cell, sub_range, data))

        result = np.empty([s.stop - s.start for s in array_slice], dtype=dtype)
        offset = [s.start for s in array_slice]

        for cell, sub_range, data in results:
            t = [slice(x.start-o, x.stop-o) if isinstance(x, slice) else x-o for x, o in
                 zip(cell+tuple(sub_range), offset)]
            if data.dtype != dtype:
                data = np.frombuffer(data, dtype=dtype, count=-1, offset=0)
            result[t] = data.reshape([s.stop - s.start for s in sub_range])

        return result

    # def get_slice_by_bbox(self, slice, shape, dtype, s3_bucket, s3_key):
    #     # get bbox range from (first_slice.start, last_slice.stop)
    #     # pull out from S3, data is offset by first_slice.start
    #     # use get_slice2 to index and retrieve data.

    #     s3_start = [s.start for s in slice]
    #     s3_stop = [s.stop for s in slice]

    #     d = self.s3io.get_byte_range_mp(s3_bucket, s3_key, s3_start, s3_end)

    #     # calculate offsets using s3_start
    #     # call get_slice to get data from numpy array
    #     # build result array
    #     # return

    # def shape_to_idx(self, slices):
    #     dims_but_last = slices[:-1]
    #     ranges = [range(0, s) for s in slices]
    #     cell_addresses = list(itertools.product(*ranges))
    #     return cell_addresses

    # def slice_to_idx(self, slices):
    #     # slices_but_last = slices[:-1]
    #     ranges = [range(s.start, s.stop) for s in slices]
    #     cell_addresses = list(itertools.product(*ranges))
    #     return cell_addresses
