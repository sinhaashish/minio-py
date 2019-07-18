# -*- coding: utf-8 -*-
# MinIO Python Library for Amazon S3 Compatible Cloud Storage, (C)
# 2015, 2016, 2017, 2018, 2019 MinIO, Inc.
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

from  .select_object import extract_message, extract_message1



remaining =  bytearray() 
class SelectObjectReader(object):    
    def __init__(self, response,):
        self.response = response
        self.stats = {}
    def readable(self):
        return True

    def writeable(self):
        return False

    @property
    def closed(self):
        return self.response.isclosed()

    def close(self):
        self.response.close()

    def stats(self):
        return self.stats
  
    def progress(self):
        return self.progress

    def read(self, num_bytes):
    #    ... extract each record from the response body ... and buffer it.
    #    ... send only up to requested bytes such as message[:num_bytes]
    #    ... rest is buffered and added to the next iteration.
        
        global remaining
        if len(remaining) == 0 :
            res =  extract_message1(self.response)
            # print(" harai sab peera",len(res))
            # return ""
            if len(res) == 0:
                return ""            
            else :
                remaining = res
        
        if num_bytes < len(remaining):
            result = remaining[:num_bytes]
            del remaining[:num_bytes] 
            return result.decode("utf-8")
        else :
            left_in_buffer = remaining[:len(remaining)]
            del remaining[:len(left_in_buffer)]
            return left_in_buffer.decode("utf-8")

    def read1(self, num_bytes):
        p =  extract_message1(self.response)
        # print( " bas itna hai ",p)
        return p.decode("utf-8")
