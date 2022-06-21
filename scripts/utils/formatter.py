# coding=utf-8
# @Time: 2022/4/26 18:39
# @Author: forevermessi@foxmail.com
from typing import Dict
from .color import Color


class Formatter(object):
    def __init__(self, column_definition: Dict[str, int]):
        self.__column_definition = column_definition
        self.__column_name_format = ''
        for k, v in self.__column_definition.items():
            self.__column_name_format += f'%-{v}s'

    def print_header(self):
        header_columns = list()
        header_cutlines = list()
        for k, v in self.__column_definition.items():
            header_columns.append(k)

            def line_add_func(length):
                line = ''
                for i in range(length):
                    line += '-'
                return line

            header_cutlines.append(line_add_func(len(k)))
        c = Color()
        c.print_yellow(self.__column_name_format % tuple(header_columns))
        c.print_yellow(self.__column_name_format % tuple(header_cutlines))

    def print_line(self, column_values: list):
        """
        format print
        """
        if len(column_values) != len(self.__column_definition):
            raise Exception("Number of input values mismatch with formatter!")
        print(self.__column_name_format % tuple(column_values))


if __name__ == '__main__':
    f = Formatter(column_definition={"Name": 10, "Age": 5, "Address": 64})
    f.print_header()
    f.print_line(("Cen", 111, "SH.CN"))
    print("")
    f.print_header()
    f.print_line(("Lee", 222, "LA.USA"))
