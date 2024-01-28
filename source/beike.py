# encoding:utf-8

from html.parser import HTMLParser
from source.common import getHtml


class BeiKeParseBaseInfo(HTMLParser):
    def __init__(self):
        super().__init__()
        self.totalPage = 1

    def handle_starttag(self, tag, attrs):
        if tag == "div" and ("class", "page-box house-lst-page-box") in attrs:
            # print("Encountered a page-data attr:", attrs)
            for attr in attrs:
                if attr[0] == 'page-data':
                    print("total page number is ", eval(attr[1])['totalPage'])
                    self.totalPage = eval(attr[1])['totalPage']

    def handle_endtag(self, tag):
        return

    def handle_data(self, data):
        return


class BeikeParser(HTMLParser):
    def __init__(self, html_data, first_page_url):
        super().__init__()
        # 获取total page number
        self.html_data = html_data
        self.BeiKeBaseParser = BeiKeParseBaseInfo()
        self.BeiKeBaseParser.feed(data=self.html_data)
        self.totalPageNumber = self.BeiKeBaseParser.totalPage
        # 首页url
        self.first_page_url = first_page_url
        # 存储中间数据(链家为总房价与单价)
        self.span = ""
        # 房屋名称
        self.houseName = []
        # 小区名称
        self.villageName = []
        # 房子介绍
        self.houseNote = []
        # 总价
        self.houseTotlePrice = []
        self.houseTotlePrice_tmp = ""
        # 单价
        self.houseUnitPrice = []
        # 房屋链接
        self.houseLink = []
        # 第一张图片
        self.houseImg = []
        # 关注人数
        self.followNum = []
        # 城市缩写
        self.city = []
        # 用于标记数据类型
        self.flag = []
        self.sign = 0

    def _get_page_url(self, page_num):
        first_page_url_list = self.first_page_url.split('/')
        # add page number to url
        first_page_url_list[-2] = f'pg{page_num}' + first_page_url_list[-2]
        current_page_url = '/'.join(first_page_url_list)
        return current_page_url

    def feed(self):
        for page_num in range(1, self.totalPageNumber+1):
            current_page_url = self._get_page_url(page_num=page_num)
            html_info = getHtml(current_page_url)
            super().feed(html_info)
            print(f"Beike Finish feed page num {page_num}")
        # 校验数据个数是否统一
        size = len(self.houseName)
        if len(self.houseName) != size \
            or len(self.villageName) != size \
            or len(self.houseNote) != size \
            or len(self.houseTotlePrice) != size \
            or len(self.houseUnitPrice) != size \
            or len(self.houseLink) != size \
            or len(self.houseImg) != size \
            or len(self.followNum) != size \
                or len(self.city) != size:
            raise ValueError(
                "数据个数不一致：houseName-" + str(len(self.houseName)) +
                ",villageName-" + str(len(self.villageName)) +
                ",houseNote-" + str(len(self.houseNote)) +
                ",houseTotlePrice-" + str(len(self.houseTotlePrice)) +
                ",houseUnitPrice-" + str(len(self.houseUnitPrice)) +
                ",houseLink-" + str(len(self.houseLink)) +
                ",houseImg-" + str(len(self.houseImg)) +
                ",followNum-" + str(len(self.followNum)) +
                ",city-" + str(len(self.city))
                    )
        return self.houseName, \
            self.villageName, \
            self.houseNote, \
            self.houseTotlePrice, \
            self.houseUnitPrice, \
            self.houseLink, \
            self.houseImg, \
            self.followNum, \
            self.city,

    def handle_starttag(self, tag, attrs):
        if tag == "span":
            if ("class", "houseIcon") in attrs:
                self.flag.append("houseNote")
            self.flag.append("span")
        elif tag == "a" and\
                ("class", "img VIEWDATA CLICKDATA maidian-detail") in attrs:
            # self.flag.append("houseName")
            for attr in attrs:
                if attr[0] == "title":
                    self.houseName.append(attr[1])
                elif attr[0] == "href":
                    house_link = attr[1]
                    city = house_link[
                        house_link.find('//')+2:house_link.find('ke.com')-1
                        ].upper()
                    self.houseLink.append(house_link)
                    self.city.append(city)
        # elif tag == "a" and ("data-el", "region") in attrs:
        #     self.flag.append("villageName")
        # elif tag == "a" and ("class", "no_resblock_a") in attrs:
        #     self.flag.append("villageName")
        # elif tag == "div" and ("class", "houseInfo") in attrs:
        #     self.flag.append("houseNote")
        elif tag == "div" and ("class", "totalPrice totalPrice2") in attrs:
            self.flag.append("houseTotlePrice_2")
        elif tag == "div" and ("class", "unitPrice") in attrs:
            self.flag.append("houseUnitPrice_2")
        elif tag == "img" and ("class", "lj-lazy") in attrs:
            for attr in attrs:
                if attr[0] == "alt":
                    for attr2 in attrs:
                        if attr2[0] == "data-original":
                            self.houseImg.append(attr2[1])
                            break
                    break
        elif tag == "div" and ("class", "positionInfo") in attrs:
            self.flag.append("villageName_1")
        elif tag == "a" and\
            len(self.flag) > 0 and\
                self.flag[-1] == "villageName_1":
            self.flag.pop()
            self.flag.append("villageName_2")
        elif tag == "div" and ("class", "followInfo") in attrs:
            self.flag.append("followNum")

    def handle_data(self, data):
        data = data.replace(' ', '')
        if len(self.flag) > 0:
            if self.flag[-1] == "span":
                self.span = data
                self.flag.pop()
                if len(self.flag) > 0 and self.flag[-1] == "houseUnitPrice_2":
                    self.houseUnitPrice.append(self.span)
                    self.flag.pop()
                elif len(self.flag) > 0 and self.flag[-1] == "houseNote":
                    self.houseNote.append(self.span)
                    # self.villageName.append(self.span.split('|')[0].strip())
                    self.flag.pop()
                elif len(self.flag) > 0 and self.flag[-1] == "followNum":
                    self.followNum.append(
                        int(self.span.replace(' ', '').split('人')[0]))
                    self.flag.pop()
                elif len(self.flag) > 0 and\
                        self.flag[-1] == "houseTotlePrice_2":
                    self.houseTotlePrice_tmp = self.span
                    # self.villageName.append(self.span.split('|')[0].strip())
            elif self.flag[-1] == "houseName":
                # print(str(data))
                self.houseName.append(data)
                self.flag.pop()
            # elif self.flag[-1] == "villageName":
            #     # print(str(data))
            #     self.villageName.append(data)
            #     self.flag.pop()
            # elif self.flag[-1] == "houseNote":
            #     print(self.span)
            #     self.houseNote.append(self.span)
            #     self.villageName.append(self.span.split('|')[0])
            #     self.span = ""
            #     self.flag.pop()
            elif self.flag[-1] == "houseTotlePrice_2":
                if data != "":
                    self.houseTotlePrice_tmp = \
                        self.houseTotlePrice_tmp + self.span + data
                self.span = ""
                # self.flag.pop()
            elif self.flag[-1] == "villageName_2":
                # print(str(data))
                self.villageName.append(data)
                self.flag.pop()

    def handle_endtag(self, tag):
        if tag == "div" \
            and len(self.flag) > 0 \
                and self.flag[-1] == "houseTotlePrice_2":
            self.houseTotlePrice.append(
                self.houseTotlePrice_tmp
                )
            self.houseTotlePrice_tmp = ""
            self.flag.pop()
