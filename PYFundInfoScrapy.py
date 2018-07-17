from urllib import request
import PYDBConnect


'''
    http 常量设置
'''
target_url = "http://fund.eastmoney.com/js/fundcode_search.js"
target_headers = [('Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.1 Safari/605.1.15'),
                  ('Connection', 'keep-alive'),
                  ('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'),
                  ('Accept-Language', 'Accept-Language')]
target_proxy = {'https': '123.139.56.238:999'}


'''
    访问URL，获取报文，并将报文按照基金进行分组，返回基金信息的数组类型
'''
def get_fundbasicinfo(url, headers, proxy):

    tfundinfodict = {}

    proxy_support = request.ProxyHandler(proxy)
    opener = request.build_opener(proxy_support)
    opener.addheaders = headers
    request.install_opener(opener)



    with request.urlopen(url) as response:
        html = response.read().decode("utf-8")
        all_fundinfo_text = html.lstrip('﻿var r = [[').rstrip(']];')

        fundinfolist = all_fundinfo_text.split('],[')

        for rowfundinfo in fundinfolist:

            tfundcode = str(rowfundinfo).split(',')[0].strip('"')
            tfundname = str(rowfundinfo).split(',')[2].strip('"')
            tfundtype = str(rowfundinfo).split(',')[3].strip('"')

            #print(tfundcode + ":" + tfundname + ":" + tfundtype)
            tfundinfodict[tfundcode] = tfundname + "," + tfundtype

    return tfundinfodict


'''
    插入更新数据库中的基金信息表（插入或更新）
'''
def insertORupdate_tfundinfo(tfundcode, tfundname, tfundtype):

    conn = PYDBConnect.mysqldbConnect(PYDBConnect.pydbusername,
                                      PYDBConnect.pydbpassword,
                                      PYDBConnect.pydbname,
                                      PYDBConnect.pydbhost,
                                      PYDBConnect.pydbport)
    querycmd = " select * from TFUNDINFO where c_fundcode = '" + tfundcode + "'"
    updatecmd = "update TFUNDINFO set c_fundname = '" + tfundname + "' , c_fundtype = '" + tfundtype + "' where c_fundcode = '" + tfundcode +"'"
    insertcmd = "insert into TFUNDINFO (c_fundcode, c_fundname, c_fundtype) values ('" + tfundcode +  "', '" + tfundname + "', '" + tfundtype + "')"

    try:
        rowcount, rs = PYDBConnect.mysqldbQuery(conn, querycmd)
        if (rowcount >= 1):
            for row in rs:
                if (tfundname != str(row[2])) or (tfundtype != str(row[3])):
                    PYDBConnect.mysqldbInsertDeleteUpdate(conn, updatecmd)
        else:
            PYDBConnect.mysqldbInsertDeleteUpdate(conn, insertcmd)
    except Exception as e:
        print("insertORupdate_tfundinfo：{0}".format(str(e)))
    finally:
        PYDBConnect.mysqldbConnClose(conn)


if __name__ == "__main__":

    fundinfo_dict = get_fundbasicinfo(target_url, target_headers, target_proxy)

    if len(fundinfo_dict) > 0:
        for tfundcode, tfundname_type in fundinfo_dict.items():
            tfundname = tfundname_type.split(',')[0]
            tfundtype = tfundname_type.split(',')[1]
            insertORupdate_tfundinfo(tfundcode, tfundname, tfundtype)
