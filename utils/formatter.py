# coding=utf-8
# @Time: 2022/4/26 18:39
# @Author: forevermessi@foxmail.com
"""
column_definition={"Name": 10, "Age": 5, "Address": 64}: which means name's max len is 10, age's max len is 5 ...
column_format will be: '%-10s%-5s%-64s'
outpout:
Name      Age  Address      --header_fields
----      ---  -------      --header_cutlines
Cen       111  SH.CN        --record
Lee       222  LA.USA       --record
"""
from typing import Dict, Sequence
from utils.color import Color


class Formatter(object):
    def __init__(self, column_definition: Dict[str, int]):
        self.__column_definition = column_definition
        # format of both header and records
        self.__column_format = ''
        for field_name, field_length in self.__column_definition.items():
            self.__column_format += f'%-{field_length}s'

    @staticmethod
    def get_cutline(length):
        field_cut_line = ""
        for i in range(length):
            field_cut_line += "-"
        return field_cut_line

    def print_header(self):
        header_fields = list()
        header_cutlines = list()
        for field_name, field_length in self.__column_definition.items():
            header_fields.append(field_name)
            header_cutlines.append(self.get_cutline(len(field_name)))
        print(self.__column_format % tuple(header_fields))
        print(self.__column_format % tuple(header_cutlines))

    def print_record(self, record: Sequence):
        if len(record) != len(self.__column_definition):
            raise Exception("Number of input values mismatch with formatter!")
        print(self.__column_format % tuple(record))


if __name__ == '__main__':
    f = Formatter(column_definition={"Name": 10, "Age": 5, "Address": 64})
    f.print_header()
    f.print_record(("Cen", 111, "SH.CN"))
    f.print_record(("Lee", 222, "LA.USA"))