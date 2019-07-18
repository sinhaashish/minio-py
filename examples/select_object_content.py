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
from minio import SelectObjectReader
from minio.error import ResponseError
from minio.select_object import CRCValidationError
from minio.select_object_options import (SelectObjectOptions, CSVInput, JSONInput, RequestProgress, ParquetInput, InputSerialization, OutputSerialization, CSVOutput, JsonOutput)

# client = Minio('s3.amazonaws.com',
#                access_key='YOUR-ACCESSKEY',
#                secret_key='YOUR-SECRETKEY')
client = Minio('localhost:9000',
               access_key='minio',
               secret_key='minio123',
               secure=False)

obj = SelectObjectOptions(
    expression=" select * from s3object",
    input_serialization = InputSerialization(compression_type="NONE",
                              csv=CSVInput(FileHeaderInfo="USE",
                                           RecordDelimiter="\n",
                                           FieldDelimiter=",",
                                           QuoteCharacter='"',
                                           QuoteEscapeCharacter='"',
                                           Comments="#",
                                           AllowQuotedRecordDelimiter="FALSE"
                                           ),
                             ),      
                             # If input is JSON
                             # json = JSONInput(Type="DOCUMENT"
                             #                 )
                             #         ),

    output_serialization=OutputSerialization(
                        csv = CSVOutput(
                            QuoteFields="ASNEEDED",
                            RecordDelimiter="\n",
                            FieldDelimiter=",",
                            QuoteCharacter='"',
                            QuoteEscapeCharacter='"',
                            )
                        ),

    # If output is JSON
    # output_serialization=OutputSerialization(
    #                 json = JsonOutput(
    #                     RecordDelimiter="\n"
    #                     )
    #                 ),
    request_progress=RequestProgress(
        enabled="TRUE"
        )
    )

try:
    response = client.select_object_content('sinha', 'test.csv', obj)
    t = SelectObjectReader(response)
    while True :
        records = t.read(150)
        print("ashish" ,records)
        if records == "" :
            break
        

    # records, stats = client.select_object_content('sinha', 'test.csv', obj)
    # with open('/home/ashish/select_result/my-record', 'wb') as record_data:
    #     record_data.write(records)
    # with open('/home/ashish/select_result/my-stat', 'wb') as stat_data:
    #     stat_data.write(stats)
except CRCValidationError as err:
    print(err)
except ResponseError as err:
    print(err)
