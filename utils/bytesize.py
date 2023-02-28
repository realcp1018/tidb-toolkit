# coding=utf-8
# @Time: 2023/2/27
# @Author: forevermessi@foxmail.com

"""
Usage:
    1.
    2.
    ...
"""


class ByteSize(object):
    BYTE_SIZE_MAP = {
        "KiB": 1 << 10,
        "MiB": 1 << 20,
        "GiB": 1 << 30,
        "TiB": 1 << 40
    }

    def __init__(self, size: str):
        self.__size = size

    def get(self):
        for unit, size_factor in self.BYTE_SIZE_MAP.items():
            if self.__size.endswith(unit):
                return float(self.__size.strip(unit)) * size_factor


if __name__ == '__main__':
    print(ByteSize("1MiB").get())
