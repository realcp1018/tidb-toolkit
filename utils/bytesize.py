# coding=utf-8
# @Time: 2023/2/27
# @Author: forevermessi@foxmail.com

"""
Usage:
    1.
    2.
    ...
"""
import re


class ByteSize(object):
    BYTE_SIZE_MAP = {
        "B": 1,
        "KiB": 1 << 10,
        "KB": 1 << 10,
        "MiB": 1 << 20,
        "MB": 1 << 20,
        "GiB": 1 << 30,
        "GB": 1 << 30,
        "TiB": 1 << 40,
        "TB": 1 << 40,
        "PiB": 1 << 50,
        "PB": 1 << 50
    }

    def __init__(self, size: str):
        self.__size = size

    def get(self) -> int:
        match = re.fullmatch(r'\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*', self.__size)
        num, unit = match.groups()
        if unit in self.BYTE_SIZE_MAP:
            return int(float(num) * self.BYTE_SIZE_MAP[unit])
        elif unit.upper() in self.BYTE_SIZE_MAP:
            return int(float(num) * self.BYTE_SIZE_MAP[unit.upper()])
        raise ValueError(f"Unsupported size unit: {unit}")


if __name__ == '__main__':
    print(ByteSize("0B").get())
    print(ByteSize("4kb").get())