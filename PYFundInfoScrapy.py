from urllib import request
from bs4 import BeautifulSoup
import PYDBConnect
import re
import time

'''
    http 常量设置
'''
target_headers = [('Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.1 Safari/605.1.15'),
                  ('Connection', 'keep-alive'),
                  ('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'),
                  ('Accept-Language', 'Accept-Language')]
target_proxy = {'https': '123.139.56.238:999'}


target_basicinfourl = 'http://fund.eastmoney.com/f10/'
target_fundlisturl = 'http://fund.eastmoney.com/js/fundcode_search.js'



'''
    访问URL，获取报文，并将报文按照基金进行分组，返回基金信息的数组类型
'''
def get_fundinfolist(url, headers, proxy):

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
                    print("Update TFUNDINFO for fundcode = " + tfundcode)
        else:
            PYDBConnect.mysqldbInsertDeleteUpdate(conn, insertcmd)
            print("Insert TFUNDINFO for fundcode = " + tfundcode)
    except Exception as e:
        print("ERROR---insertORupdate_tfundinfo：{0}".format(str(e)))
    finally:
        PYDBConnect.mysqldbConnClose(conn)


'''
    访问URL，获取报文，获取基金管理人、基金经理、基金规模、成立日期、最新报告日期等信息
'''
def get_fundbasicinfo(url, headers, proxy, tfundcode):

    tfundcompany = ''
    tfundmanager = ''
    tfundasset = ''
    tfundreport_date = ''
    tfundsetup_date = ''

    target_url = url + 'jbgk_' + tfundcode + '.html'

    proxy_support = request.ProxyHandler(proxy)
    opener = request.build_opener(proxy_support)
    opener.addheaders = headers
    request.install_opener(opener)

    with request.urlopen(target_url) as response:
        html = response.read().decode('utf-8')

        bsobj = BeautifulSoup(html, 'html.parser')
        tfundinfo = bsobj.find('table', class_='info w790')
        bs_fundinfo = BeautifulSoup(str(tfundinfo), 'html.parser')

        bs_fundcompany = bs_fundinfo.find('a', href=re.compile('(http://fund.eastmoney.com/company/).*\.html'))
        tfundcompany = bs_fundcompany.text

        bs_fundmanagers = bs_fundinfo.find_all('a', href=re.compile('(http://fund.eastmoney.com/manager/).*\.html'))
        for bs_fundmanager in bs_fundmanagers:
            tfundmanager += (bs_fundmanager.text+"|")

        bs_fundassetrepdate = bs_fundinfo.find('th', text='资产规模').nextSibling
        tfundassetrepdate = str(bs_fundassetrepdate.text).split('份额规模')[0].strip()
        if(len(str(tfundassetrepdate).split('亿元（截止至：'))>1):
            tfundasset = str(tfundassetrepdate).split('亿元（截止至：')[0]
            tfundreport_date = str(tfundassetrepdate).split('亿元（截止至：')[1].strip('日）').replace('年', '-').replace('月', '-')
        else:
            tfundasset = '0.00'
            tfundreport_date = '0001-01-01'


        bs_setupdate = bs_fundinfo.find('th', text='成立日期/规模').nextSibling
        if(len(str(bs_setupdate.text).split(' / '))>1):
            tfundsetup_date = str(bs_setupdate.text).split(' / ')[0].strip('日').replace('年', '-').replace('月', '-')
        else:
            tfundsetup_date = '0001-01-01'

        if tfundsetup_date == '':
            tfundsetup_date = '0001-01-01'

    return tfundcompany, tfundmanager, tfundasset, tfundreport_date, tfundsetup_date


'''
    更新TFUNDINFO中基金管理人、基金成立日期等信息
'''
def updateFundCompanyAndSetupDate(fundcode, fundcomany, spdate):

    conn = PYDBConnect.mysqldbConnect(PYDBConnect.pydbusername,
                                      PYDBConnect.pydbpassword,
                                      PYDBConnect.pydbname,
                                      PYDBConnect.pydbhost,
                                      PYDBConnect.pydbport)

    updatecmd = "update TFUNDINFO set C_FUNDCOMPANY = '" + fundcomany + "' , D_SETPUPDATE = '" + spdate + "' where c_fundcode = '" + fundcode + "'"

    try:
        PYDBConnect.mysqldbInsertDeleteUpdate(conn, updatecmd)
        print(time.strftime("%Y-%m-%d %H:%M:%S" + ": Update TFUNDINFO for Fundcode = " + fundcode)
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

    querycmd = 'select C_FUNDCODE from TFUNDINFO where C_FUNDCOMPANY is null and D_SETPUPDATE is null'
    try:
        rowcount, rs = PYDBConnect.mysqldbQuery(conn, querycmd)
    except Exception as e:
        print("ERROR---selectNullCompanyAndSPdate：{0}".format(str(e)))
    finally:
        PYDBConnect.mysqldbConnClose(conn)

    return rowcount, rs

if __name__ == "__main__":

    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "： Start to scrapy TFUNDINFO...")

    #更新基金基本信息：基金代码、基金名称、基金类型等
    fundinfo_dict = get_fundinfolist(target_fundlisturl, target_headers, target_proxy)

    if len(fundinfo_dict) > 0:
        for tfundcode, tfundname_type in fundinfo_dict.items():
            tfundname = tfundname_type.split(',')[0]
            tfundtype = tfundname_type.split(',')[1]
            insertORupdate_tfundinfo(tfundcode, tfundname, tfundtype)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": Update TFUNDINFO with fundcode, fundname, fundtype Done!")


    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "： Start to fill TFUNDINFO with fundcompany, setupdate ...")
    #更新TFUNDINFO基金管理人与成立日期为空的记录
    rownum, resCol = selectNullCompanyAndSPdate()
    if (rownum > 0):
        for rs in resCol:
            ofundcode = str(rs[0])
            ofundcompany, ofundmanager, ofundasset, ofundreport_date, ofundsetup_date = get_fundbasicinfo(target_basicinfourl,
                                                                                                  target_headers,
                                                                                                  target_proxy,
                                                                                                  ofundcode)
            updateFundCompanyAndSetupDate(ofundcode, ofundcompany, ofundsetup_date)
            time.sleep(1)

    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": Update TFUNDINFO with fundcompany, setupdate Done!")