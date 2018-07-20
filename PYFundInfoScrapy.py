import re
import socket
import time
from urllib import request

from bs4 import BeautifulSoup

import PYDBConnect

timeout = 20
socket.setdefaulttimeout(timeout)

'''
    http 常量设置
'''
target_headers = [('User-Agent',
                   'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36'),
                  ('Connection', 'keep-alive'),
                  ('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'),
                  ('Accept-Language', 'zh-cn')]
target_proxy = {'https': '1115.198.37.85:6666'}

target_basicinfourl = 'http://fund.eastmoney.com/f10/'
target_fundlisturl = 'http://fund.eastmoney.com/js/fundcode_search.js'

'''
    访问URL，获取报文，并将报文按照基金进行分组，返回基金信息的数组类型
'''


def get_fundinfolist(url, headers, proxy):
    tfundinfodict = {}
    response = False
    html = None

    proxy_support = request.ProxyHandler(proxy)
    opener = request.build_opener(proxy_support)
    opener.addheaders = headers
    request.install_opener(opener)

    try:
        response = request.urlopen(url)
        html = response.read().decode("utf-8")
    except Exception as e:
        print("Fail to open Url: " + url)
        print(str(e))
        print("Try to open Url again : " + url)
        try:
            response = request.urlopen(url)
            html = response.read().decode("utf-8")
        except Exception as e:
            print("Fail to open Url: " + url)
            print(str(e))
            print("End trying to open it")
    finally:
        opener.close()
        response.close()

    all_fundinfo_text = html.lstrip('﻿var r = [[').rstrip(']];')
    fundinfolist = all_fundinfo_text.split('],[')

    for rowfundinfo in fundinfolist:
        tfundcode = str(rowfundinfo).split(',')[0].strip('"')
        tfundname = str(rowfundinfo).split(',')[2].strip('"')
        tfundtype = str(rowfundinfo).split(',')[3].strip('"')

        tfundinfodict[tfundcode] = tfundname + "," + tfundtype

    return tfundinfodict


'''
    访问URL，获取报文，获取基金管理人、基金经理、基金规模、成立日期、最新报告日期等信息
'''


def get_fundbasicinfo(url, headers, proxy, tfundcode):
    tfundcompany = ''
    tfundmanager = ''
    tfundasset = ''
    tfundreport_date = ''
    tfundsetup_date = ''

    response = False
    html = None

    target_url = url + 'jbgk_' + tfundcode + '.html'

    proxy_support = request.ProxyHandler(proxy)
    opener = request.build_opener(proxy_support)
    opener.addheaders = headers
    request.install_opener(opener)

    try:
        response = request.urlopen(target_url)
        html = response.read().decode('utf-8')
    except Exception as e:
        print("Fail to open Url: " + url)
        print(str(e))
        print("Try to open Url again : " + url)
        try:
            response = request.urlopen(url)
            html = response.read().decode("utf-8")
        except Exception as e:
            print("Fail to open Url: " + url)
            print(str(e))
            print("End trying to open it")
    finally:
        opener.close()
        response.close()

    bsobj = BeautifulSoup(html, 'html.parser')
    tfundinfo = bsobj.find('table', class_='info w790')
    bs_fundinfo = BeautifulSoup(str(tfundinfo), 'html.parser')

    bs_fundcompany = bs_fundinfo.find('a', href=re.compile('(http://fund.eastmoney.com/company/).*\.html'))
    tfundcompany = bs_fundcompany.text

    bs_fundmanagers = bs_fundinfo.find_all('a', href=re.compile('(http://fund.eastmoney.com/manager/).*\.html'))
    for bs_fundmanager in bs_fundmanagers:
        tfundmanager += (bs_fundmanager.text + "|")

    bs_fundassetrepdate = bs_fundinfo.find('th', text='资产规模').nextSibling
    tfundassetrepdate = str(bs_fundassetrepdate.text).split('份额规模')[0].strip()
    if (len(str(tfundassetrepdate).split('亿元（截止至：')) > 1):
        tfundasset = str(tfundassetrepdate).split('亿元（截止至：')[0]
        tfundreport_date = str(tfundassetrepdate).split('亿元（截止至：')[1].strip('日）').replace('年', '-').replace('月', '-')
    else:
        tfundasset = '0.00'
        tfundreport_date = '0001-01-01'

    bs_setupdate = bs_fundinfo.find('th', text='成立日期/规模').nextSibling
    if (len(str(bs_setupdate.text).split(' / ')) > 1):
        tfundsetup_date = str(bs_setupdate.text).split(' / ')[0].strip('日').replace('年', '-').replace('月', '-')
    else:
        tfundsetup_date = '0001-01-01'

    if tfundsetup_date == '':
        tfundsetup_date = '0001-01-01'

    return tfundcompany, tfundmanager, tfundasset, tfundreport_date, tfundsetup_date


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
    updatecmd = "update TFUNDINFO set c_fundname = '" + tfundname + "' , c_fundtype = '" + tfundtype + "' where c_fundcode = '" + tfundcode + "'"
    insertcmd = "insert into TFUNDINFO (c_fundcode, c_fundname, c_fundtype) values ('" + tfundcode + "', '" + tfundname + "', '" + tfundtype + "')"

    try:
        rowcount, rs = PYDBConnect.mysqldbQuery(conn, querycmd)
        if (rowcount >= 1):
            for row in rs:
                if (tfundname != str(row[2])) or (tfundtype != str(row[3])):
                    PYDBConnect.mysqldbInsertDeleteUpdate(conn, updatecmd)
                    print("Update TFUNDINFO for fundcode = " + tfundcode)
        else:
            PYDBConnect.mysqldbInsertDeleteUpdate(conn, insertcmd)
            print("Insert TFUNDINFO for fundcode = " + tfundcode)
    except Exception as e:
        print("ERROR---insertORupdate_tfundinfo：{0}".format(str(e)))
    finally:
        PYDBConnect.mysqldbConnClose(conn)


'''
    插入数据库中的基金报告表（插入或更新）
'''


def insert_tfundreport(tfundcode, treportdate, tfundmanager, tfundasset):
    conn = PYDBConnect.mysqldbConnect(PYDBConnect.pydbusername,
                                      PYDBConnect.pydbpassword,
                                      PYDBConnect.pydbname,
                                      PYDBConnect.pydbhost,
                                      PYDBConnect.pydbport)

    querycmd = "select * from TFUNDREPORT where c_fundcode = '" + tfundcode + "'" + " and d_reportdate = '" + treportdate + "'"
    insertcmd = "insert into TFUNDREPORT (c_fundcode, d_reportdate, c_fundmanager, c_fundasset) \
                                  values ('" + tfundcode + "', '" + treportdate + "', '" + tfundmanager + "', '" + tfundasset + "')"

    try:
        rowcount, rs = PYDBConnect.mysqldbQuery(conn, querycmd)
        if (rowcount == 0):
            PYDBConnect.mysqldbInsertDeleteUpdate(conn, insertcmd)
            print("Insert TFUNDREPORT for fundcode = " + tfundcode + ", reportdate = " + treportdate)
    except Exception as e:
        print("ERROR---insert_tfundreport: {0}".format(str(e)))
    finally:
        PYDBConnect.mysqldbConnClose(conn)


'''
    更新TFUNDINFO中基金管理人、基金成立日期等信息
'''


def updateFundCompanyAndSetupDate(fundcode, fundcomany, spdate):
    conn = PYDBConnect.mysqldbConnect(PYDBConnect.pydbusername,
                                      PYDBConnect.pydbpassword,
                                      PYDBConnect.pydbname,
                                      PYDBConnect.pydbhost,
                                      PYDBConnect.pydbport)

    updatecmd = "update TFUNDINFO set C_FUNDCOMPANY = '" + fundcomany + "', D_SETUPDATE = '" + spdate + "' where c_fundcode = '" + fundcode + "'"

    try:
        PYDBConnect.mysqldbInsertDeleteUpdate(conn, updatecmd)
    except Exception as e:
        print("ERROR---updateFundCompanyAndSetupDate：{0}".format(str(e)))
    finally:
        PYDBConnect.mysqldbConnClose(conn)


'''
    找出TFUNDINFO中基金管理人与基金成立日期为空的记录
'''


def selectNullCompanyAndSPdate():
    rowcount = 0
    rs = {}

    conn = PYDBConnect.mysqldbConnect(PYDBConnect.pydbusername,
                                      PYDBConnect.pydbpassword,
                                      PYDBConnect.pydbname,
                                      PYDBConnect.pydbhost,
                                      PYDBConnect.pydbport)

    querycmd = "select C_FUNDCODE from TFUNDINFO where C_FUNDCOMPANY is null or D_SETUPDATE is null"
    try:
        rowcount, rs = PYDBConnect.mysqldbQuery(conn, querycmd)
    except Exception as e:
        print("ERROR---selectNullCompanyAndSPdate：{0}".format(str(e)))
    finally:
        PYDBConnect.mysqldbConnClose(conn)

    return rowcount, rs


'''
    获取TFUNDINFO中所有基金的基金代码
'''


def selectAllFundcode():
    rowcount = 0
    rs = {}

    conn = PYDBConnect.mysqldbConnect(PYDBConnect.pydbusername,
                                      PYDBConnect.pydbpassword,
                                      PYDBConnect.pydbname,
                                      PYDBConnect.pydbhost,
                                      PYDBConnect.pydbport)
    querycmd = "select c_fundcode from TFUNDINFO"

    try:
        rowcount, rs = PYDBConnect.mysqldbQuery(conn, querycmd)
    except Exception as e:
        print("ERROR---selectAllFundcode：{0}".format(str(e)))
    finally:
        PYDBConnect.mysqldbConnClose(conn)

    return rowcount, rs


if __name__ == "__main__":

    # 更新基金基本信息：基金代码、基金名称、基金类型等
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "： Start to scrapy TFUNDINFO...")
    fundinfo_dict = get_fundinfolist(target_fundlisturl, target_headers, target_proxy)

    if len(fundinfo_dict) > 0:
        for tfundcode, tfundname_type in fundinfo_dict.items():
            tfundname = tfundname_type.split(',')[0]
            tfundtype = tfundname_type.split(',')[1]
            insertORupdate_tfundinfo(tfundcode, tfundname, tfundtype)
    print(time.strftime("%Y-%m-%d %H:%M:%S",
                        time.localtime()) + ": Update TFUNDINFO with fundcode, fundname, fundtype Done!")

    # 更新TFUNDINFO基金管理人为空或成立日期为'0001-01-01'的记录
    print(time.strftime("%Y-%m-%d %H:%M:%S",
                        time.localtime()) + ": Start to fill TFUNDINFO with fundcompany, setupdate ...")
    rownum, resCol = selectNullCompanyAndSPdate()
    if (rownum > 0):
        for rs in resCol:
            ofundcode = str(rs[0])
            ofundcompany, ofundmanager, ofundasset, ofundreport_date, ofundsetup_date = get_fundbasicinfo(
                target_basicinfourl,
                target_headers,
                target_proxy,
                ofundcode)
            updateFundCompanyAndSetupDate(ofundcode, ofundcompany, ofundsetup_date)
            time.sleep(1)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": Update TFUNDINFO with fundcompany, setupdate Done!")

    # 更新基金报告表：基金代码、基金报告期、基金经理、基金规模
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": Start to scrapy TFUNDREPORT...")
    row_num, res_Col = selectAllFundcode()
    if (row_num > 0):
        for res in res_Col:
            o_fundcode = str(res[0])
            o_fundcompany, o_fundmanager, o_fundasset, o_fundreportdate, o_fundsetupdate = get_fundbasicinfo(
                target_basicinfourl,
                target_headers,
                target_proxy,
                o_fundcode)
            insert_tfundreport(o_fundcode, o_fundreportdate, o_fundmanager, o_fundasset)
            time.sleep(1)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": Update TFUNDREPORT Done!")
