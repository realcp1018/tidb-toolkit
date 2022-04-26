# coding=utf-8
# @Time: 2022/4/26 18:39
# @Author: forevermessi@foxmail.com
from typing import Dict


class Formatter(object):
    def __init__(self, column_definition: Dict[str, int]):
        """
        输入参数格式类似{column_name: column_max_len}， 例如{"name": 20， "sex": 1}
        其中长度部分除数据长度外，还要考虑制表符部分的长度，例如假设name最大长度为15，那么剩下的长度5就相当于两列之间的空格长度
        """
        self.__column_definition = column_definition
        # 通用格式化占位符
        self.__column_name_format = ''
        for k, v in self.__column_definition.items():
            self.__column_name_format += f'%-{v}s'
        # 理想中的格式化打印结果为：
        # name     age    address   -->列名，列名长度+间距=column_max_len
        # ----     ---    -------   -->分割线,其长度等同于各自列名的长度
        # myname   11     ShangHai  -->具体数据，记录长度+间距=column_max_len

    def print_header(self):
        header_columns = list()     # 列名打印
        header_cutlines = list()    # 分割线打印
        for k, v in self.__column_definition.items():
            header_columns.append(k)

            def line_add_func(length):
                line = ''
                for i in range(length):
                    line += '-'
                return line

            header_cutlines.append(line_add_func(len(k)))
        print(self.__column_name_format % tuple(header_columns))
        print(self.__column_name_format % tuple(header_cutlines))

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
