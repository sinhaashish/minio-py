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

import io
from PyCRC.CRC32 import CRC32

from .helpers import (READ_BUFFER_SELECT, CONS_READ_SIZE, RECORDS,
                      PROGRESS, STATS, EVENT, END, ERROR
                      )


class CRCValidationError(Exception):
    '''
    Raised in case of CRC mismatch
    '''

def calcuate_crc(value):
    '''
    Returns the CRC using PyCRC
    '''
    return CRC32().calculate(value)


def validate_crc(current_value, expected_value):
    '''
    Validate through CRC check
    '''
    crc_current = calcuate_crc(current_value)
    crc_expected = byte_int(expected_value)
    if crc_current == crc_expected:
        return True
    return False


def byte_int(data_bytes):
    '''
    Convert bytes to big-endian integer
    '''
    return int.from_bytes(data_bytes, byteorder='big')


def extract_header(header, header_length):
    '''
    populates the header map after reading the header
    '''
    header_map = {}
    header_byte_parsed = 0
    # While loop ends when all the headers present are read
    #header contains multipe headers
    while header_byte_parsed < header_length:
        header_name_byte_length \
        = byte_int(header[header_byte_parsed: header_byte_parsed+1])
        header_byte_parsed += 1
        header_name \
        = header[header_byte_parsed: header_byte_parsed+header_name_byte_length]
        header_byte_parsed += header_name_byte_length
        # Header Value Type is of 1 bytes and is skipped
        header_byte_parsed += 1
        value_string_byte_length \
        = byte_int(header[header_byte_parsed: header_byte_parsed+2])
        header_byte_parsed += 2
        header_value \
        = header[header_byte_parsed:header_byte_parsed+value_string_byte_length]
        header_byte_parsed += value_string_byte_length
        header_map[header_name.decode("utf-8").lstrip(":")] = \
            header_value.decode("utf-8").lstrip(":")
    return header_map



def parse_message(header_map, payload, payload_length, record, stat):
    '''
    Parses the message
    '''
    if header_map["message-type"] == ERROR :
        error = header_map["error-code"] + ":\"" +\
                header_map["error-message"] + "\""
        record.write(error)
    if header_map["message-type"] == EVENT :
        # Fetch the content-type
        content_type = header_map["content-type"]
        # Fetch the event-type
        event_type = header_map["event-type"]
        if event_type == RECORDS:
            rec = payload[0:payload_length]
            record.write(rec)
        elif  event_type == PROGRESS:
            if content_type == "text/xml":
                progress = payload[0:payload_length]
                stat.write(progress)
        elif event_type == STATS:
            if content_type == "text/xml":
                stats = payload[0:payload_length]
                stat.write(stats)

def extract_component(response, read_buffer, bytes_parsed,
                      size, chunked_message):
    '''
    Fetches all the individual component of message namely
    1. total_byte_length
    2. headers_byte_length
    3. prelude_crc
    4. header
    5. payload
    6. message_crc
    '''
    flag = False
    if read_buffer - bytes_parsed < size:
        value = chunked_message[bytes_parsed:bytes_parsed + \
                                (read_buffer - bytes_parsed)]
        rem_bytes = response.read(size- (read_buffer - bytes_parsed))
        if len(rem_bytes) > 0:
            chunked_message = response.read(read_buffer)
            flag = True
        value += rem_bytes
    else:
        value = chunked_message[bytes_parsed:bytes_parsed + size]
    if flag:
        bytes_parsed = 0
    else:
        bytes_parsed += size
    return  value, bytes_parsed, chunked_message


def extract_message(response):
    '''
    Process the response sent from server.
    Refer https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
    '''
    read_buffer = READ_BUFFER_SELECT
    record = io.BytesIO()
    stats = io.BytesIO()
    # Response read in chunks of read_buffer
    chunked_message = response.read(read_buffer)
    bytes_parsed = 0
    while bytes_parsed < read_buffer:
        #Process total byte length
        total_byte_length, bytes_parsed, chunked_message = \
            extract_component(response, read_buffer,
                              bytes_parsed, CONS_READ_SIZE,
                              chunked_message)

        #Process header byte length
        headers_byte_length, bytes_parsed, chunked_message = \
            extract_component(response, read_buffer,
                              bytes_parsed, CONS_READ_SIZE, chunked_message)

        #Process prelude CRC
        prelude_crc, bytes_parsed, chunked_message = \
            extract_component(response, read_buffer,
                              bytes_parsed, CONS_READ_SIZE, chunked_message)

        # Validate CRC for first 12 bytes
        if validate_crc(total_byte_length + headers_byte_length, prelude_crc):
            if byte_int(headers_byte_length) > 0:
                # Process header and populate header map
                header, bytes_parsed, chunked_message =  \
                    extract_component(response, read_buffer,
                                      bytes_parsed,
                                      byte_int(headers_byte_length),
                                      chunked_message)
                header_map = \
                extract_header(header, byte_int(headers_byte_length))

                # Process Payload
                payload_length = byte_int(total_byte_length) \
                                - byte_int(headers_byte_length) - int(16)
                payload, bytes_parsed, chunked_message = \
                    extract_component(response, read_buffer,
                                      bytes_parsed, payload_length,
                                      chunked_message)

                if header_map["message-type"] == EVENT :
                    # Parse message only when event-type is Records, 
                    # Progress, Stats. Break the loop if event type is End
                    # Do nothing if event type is Cont
                    if header_map["event-type"] == RECORDS or \
                        header_map["event-type"] == PROGRESS or \
                        header_map["event-type"] == STATS:
                        parse_message(header_map, payload, payload_length, record, stats)

                    if header_map["event-type"] == END :
                        break
                if header_map["message-type"] == ERROR:
                    parse_message(header_map, payload, payload_length, record, stats)
                    break
                # Fetch message CRC
                message_crc, bytes_parsed, chunked_message = \
                    extract_component(response, read_buffer,
                                      bytes_parsed, CONS_READ_SIZE,
                                      chunked_message)

                # Generate complete message
                complete_message = total_byte_length + \
                                    headers_byte_length + prelude_crc + \
                                    header + payload
                if not validate_crc(complete_message, message_crc):
                    raise CRCValidationError(
                        {"Checksum Mismatch, MessageCRC of " + \
                            str(calcuate_crc(complete_message)) + \
                            " does not equal expected CRC of "+ \
                            str(byte_int(message_crc))})
        else:
            raise CRCValidationError(
                {"Checksum Mismatch, MessageCRC of " + \
                str(calcuate_crc(total_byte_length + headers_byte_length)) + \
                " does not equal expected CRC of "+ str(byte_int(prelude_crc))})
    return record.getvalue(), stats.getvalue()
