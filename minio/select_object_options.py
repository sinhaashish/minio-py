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

"""
minio.select_object_options
~~~~~~~~~~~~~~~

This module cretaes the request for Select

:copyright: (c) 2019 by MinIO, Inc.
:license: Apache 2.0, see LICENSE for more details.

"""


class CSVInput:    
    """CSVInput.

    """
    def __init__(self, file_header_info=None, record_delimiter=None, 
                 field_delimiter=None, quote_charachter=None,
                 quote_escape_charachter=None, comments=None,
                 allow_quoted_record_delimitter=False ):
        self.file_header_info = file_header_info
        self.record_delimiter = record_delimiter
        self.field_delimiter = field_delimiter
        self.quote_charachter = quote_charachter
        self.quote_escape_charachter = quote_escape_charachter
        self.comments = comments
        self.allow_quoted_record_delimitter = allow_quoted_record_delimitter

class JSONInput:
    """JSONInput.

    """
    def __init__(self, json_type=None):
        self.json_type = json_type

class ParquetInput:
    """ParquetInput.

    """

class InputSerialization( CSVInput, JSONInput, ParquetInput):
    """InputSerialization.

    """
    def __init__(self, compression_type, CSVInput=None , JSONInput=None,
                 ParquetInput=None ):
        self.compression_type = compression_type
        self.csv_input = CSVInput
        self.json_input = JSONInput
        self.parquet_input = ParquetInput


class CSVOutput:    
    """CSVOutput.

    """
    def __init__(self, quote_field=None, record_delimiter=None, 
                 field_delimiter=None, quote_charachter=None,
                 quote_escape_charachter=None ):
        self.quote_field = quote_field
        self.record_delimiter = record_delimiter
        self.field_delimiter = field_delimiter
        self.quote_charachter = quote_charachter
        self.quote_escape_charachter = quote_escape_charachter
       
class JsonOutput:
    """JsonOutput.

    """
    def __init__(self, record_delimiter=None):
        self.record_delimiter = record_delimiter



class OutputSerialization( CSVOutput, JsonOutput):
    """OutputSerialization.

    """
    def __init__(self,  CSVOutput=None, JsonOutput=None):
        self.csv_input = CSVOutput
        self.json_input = JsonOutput
 


class RequestProgress():
    """RequestProgress.

    """
    def __init__(self, enabled=False):
        self.enabled = enabled


class  SelectObjectOptions (InputSerialization, OutputSerialization, RequestProgress):
    """SelectObjectOptions.

    """
    expression_type = 'SQL'
    def __init__(self, expression, InputSerialization, OutputSerialization, RequestProgress):
        self.expression = expression
        self.input_serialization = InputSerialization
        self.output_serialization = OutputSerialization
        self.request_progress = RequestProgress
