import boto3
import hashlib
import os
from NDATools.Utils import *


class CheckIncompleteUpload:
    def __init__(self, bucket, prefix):
        self.bucket = bucket
        self.prefix = prefix
        session = boto3.Session()
        self.client = session.client(service_name='s3')
        self.incomplete_mpu = []
        self.mpu_to_abort = {}
        self.full_file_path = {}

    def get_multipart_uploads(self):
        try:
            uploads = self.client.list_multipart_uploads(Bucket=self.bucket, Prefix=self.prefix)['Uploads']
            for u in uploads:
                if u not in self.incomplete_mpu:
                    self.incomplete_mpu.append(u)
                else:
                    self.mpu_to_abort[u['UploadId']] = u['Key']
        except KeyError:
            uploads = None

        if self.mpu_to_abort:
            self.abort_mpu()

    def abort_mpu(self):
        for upload_id, key in self.mpu_to_abort.items():
            self.client.abort_multipart_upload(
                Bucket=self.bucket, Key=key, UploadId=upload_id)

    def get_file_paths(self, directory):
        for upload in mpu.incomplete_mpu:
            key = upload['Key']
            filename = key.split(prefix + '/')
            filename = "".join(filename[1:])
            local_file = os.path.join(directory, filename)
            if os.path.isfile(local_file):
                self.full_file_path[filename] = local_file
            else:
                print("Local file to upload not found")
                sys.exit(1)

class Constants:
    PARTS = 'Parts'
    SIZE = 'Size'
    PART_NUM = 'PartNumber'
    ETAG = 'ETag'

class UploadMultiParts:
    def __init__(self, upload_obj, full_file_path, bucket, prefix):
        self.chunk_size = 0
        self.upload_obj = upload_obj
        self.full_file_path = full_file_path
        self.upload_id = self.upload_obj['UploadId']
        self.bucket = bucket
        self.key = self.upload_obj['Key']
        filename = self.key.split(prefix+'/')
        filename = "".join(filename[1:])
        self.filename = self.full_file_path[filename]
        session = boto3.Session(profile_name='dev')
        self.client = session.client(service_name='s3')
        self.completed_bytes = 0
        self.completed_parts = 0
        self.parts = []
        self.parts_completed = []

    def get_parts_information(self):
        self.upload_obj = self.client.list_parts(Bucket=self.bucket, Key=self.key,
                                             UploadId=self.upload_id)

        if Constants.PARTS in self.upload_obj:
            self.chunk_size = self.upload_obj[Constants.PARTS][0][Constants.SIZE] # size of first part should be size of all subsequent parts
            for p in self.upload_obj[Constants.PARTS]:
                try:
                    self.parts.append({Constants.PART_NUM: p[Constants.PART_NUM], Constants.ETAG: p[Constants.ETAG]})
                    self.parts_completed.append(p[Constants.PART_NUM])
                except KeyError:
                    pass

        self.completed_bytes = self.chunk_size * len(self.parts)


    def check_md5(self, part, data):

        ETag = (part[Constants.ETAG]).split('"')[1]

        md5 = hashlib.md5(data).hexdigest()

        if md5 != ETag:
            print("The file seems to be modified since previous upload attempt(md5 value does not match).")
            sys.exit(1) # force exit because file has been modified (data integrity)

    def process_uploads(self):
        seq = 1
        with open(self.filename, 'rb+') as f:
                while True:
                    buffer_start = u.chunk_size * (seq - 1)
                    f.seek(buffer_start)
                    buffer = f.read(u.chunk_size)
                    if len(buffer) == 0:  # EOF
                        break
                    if seq in self.parts_completed:
                        part = self.parts[seq - 1]
                        self.check_md5(part, buffer)
                    else:
                        self.upload_part(buffer, seq)
                    seq += 1
        self.complete()

    def upload_part(self, data, i):
        part = self.client.upload_part(Body=data, Bucket=self.bucket, Key=self.key, UploadId=self.upload_id, PartNumber=i)
        self.parts.append({Constants.PART_NUM: i, Constants.ETAG: part[Constants.ETAG]})
        self.completed_bytes += len(data)

    def complete(self):
        self.client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={Constants.PARTS: self.parts})

        print("Completed uploading {}".format(self.filename))


bucket = ""
prefix = ""
directory = ""

mpu = CheckIncompleteUpload(bucket, prefix)
mpu.get_multipart_uploads()
mpu.get_file_paths(directory)


for upload in mpu.incomplete_mpu:
    u = UploadMultiParts(upload, mpu.full_file_path, bucket, prefix)
    u.get_parts_information()
    u.process_uploads()
