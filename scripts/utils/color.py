# coding=utf-8
# @Time: 2021/1/11 10:27


class Color(object):
    class display(object):
        default = '\033[0m'
        bold = '\033[1m'
        underline = '\033[4m'
        flicker = '\033[5m'
        highlight = '\033[7m'

    class font(object):
        black = '\033[30m'
        red = '\033[31m'
        green = '\033[32m'
        yellow = '\033[33m'
        blue = '\033[34m'
        purple = '\033[35m'
        cyan = '\033[36m'
        white = '\033[37m'

    class background(object):
        black = '\033[40m'
        red = '\033[41m'
        green = '\032[42m'
        yellow = '\033[43m'
        blue = '\033[44m'
        purple = '\033[45m'
        cyan = '\033[46m'
        white = '\033[47m'

    def print_black(self, string):
        print(("{0}{1}{2}".format(self.font.black, string, self.display.default)))

    def print_red(self, string):
        print(("{0}{1}{2}".format(self.font.red, string, self.display.default)))

    def print_green(self, string):
        print(("{0}{1}{2}".format(self.font.green, string, self.display.default)))

    def print_yellow(self, string):
        print(("{0}{1}{2}".format(self.font.yellow, string, self.display.default)))

    def print_blue(self, string):
        print(("{0}{1}{2}".format(self.font.blue, string, self.display.default)))

    def print_purple(self, string):
        print(("{0}{1}{2}".format(self.font.purple, string, self.display.default)))

    def print_cyan(self, string):
        print(("{0}{1}{2}".format(self.font.cyan, string, self.display.default)))

    def print_white(self, string):
        print(("{0}{1}{2}".format(self.font.white, string, self.display.default)))

    def print_highlight(self, string):
        print(("{0}{1}{2}".format(self.display.highlight, string, self.display.default)))