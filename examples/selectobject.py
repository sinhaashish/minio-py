# -*- coding: utf-8 -*-
# MinIO Python Library for Amazon S3 Compatible Cloud Storage, (C) 2019 MinIO, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Note: YOUR-ACCESSKEYID, YOUR-SECRETACCESSKEY, my-bucketname, my-objectname and
# my-testfile are dummy values, please replace them with original values.

from minio import Minio
from minio.error import ResponseError
# from minio.error import ResponseError, ArgumentError
from minio.select_object_options import (SelectObjectOptions ,  CSVInput, JSONInput, RequestProgress, ParquetInput, InputSerialization, OutputSerialization, CSVOutput )

client = Minio('localhost:9000',
               access_key='minio',
               secret_key='minio123', secure=False)

# Get a full object
#try:
#    data = client.get_object('my-bucketname', 'my-objectname')
#    with open('my-testfile', 'wb') as file_data:
#        for d in data.stream(32*1024):
#            file_data.write(d)
#except ResponseError as err:
#    print(err)

# Select the content of object



def dump(obj):
  for attr in dir(obj):
    print("obj.%s = %r" % (attr, getattr(obj, attr)))





try:
  
    obj = SelectObjectOptions(
        expression=" select * from temp",
        InputSerialization=InputSerialization(
                        compression_type="GZIP",
                        CSVInput=CSVInput(
                            file_header_info="IGNORE",
                            record_delimiter="\\n",
                            field_delimiter=",",
                            quote_charachter=",",
                            quote_escape_charachter=",",
                            comments=",",
                            allow_quoted_record_delimitter="TRUE")
                        ),
        OutputSerialization=OutputSerialization(
                        CSVOutput=CSVOutput(
                            quote_field="ASNEEDED",
                            record_delimiter="\\n",
                            field_delimiter=",",
                            quote_charachter=",",
                            quote_escape_charachter=",",
                            )
                        ),
        RequestProgress=RequestProgress(
                        enabled="TRUE")

                )

    dump(obj)

    # data = client.select_object_content('mycsvbucket', 'mycsv.csv' , guido)
    # with open('my-testfile', 'wb') as file_data:
    #     for d in data.stream(32*1024):
    #         file_data.write(d)
except ResponseError as err:
    print(err)
