import re
import time

from bs4 import BeautifulSoup

import PYDBConnect
import PYHTTPConnect

target_baseUrl = 'http://fund.eastmoney.com/f10/'
target_fundlistUrl = 'http://fund.eastmoney.com/js/fundcode_search.js'

'''
    访问URL，获取报文，并将报文按照基金代码进行分组，返回基金信息的数组类型
'''


def get_fundinfoList(tfundListUrl):
    tfundinfodict = {}

    html = PYHTTPConnect.get_HTMLText(tfundListUrl)
    if html:
        all_fundinfo_text = html.lstrip('﻿var r = [[').rstrip(']];')
        fundinfoList = all_fundinfo_text.split('],[')

        for tfundinfo in fundinfoList:
            tfundcode = str(tfundinfo).split(',')[0].strip('"')
            tfundname = str(tfundinfo).split(',')[2].strip('"')
            tfundtype = str(tfundinfo).split(',')[3].strip('"')
            tfundinfodict[tfundcode] = tfundname + "," + tfundtype
    else:
        print("Unable to Get fundinfoList From :" + tfundListUrl)
    return tfundinfodict


'''
    访问URL，获取报文，获取基金管理人、成立日期等信息
'''


def get_fundcompanyAndSetupDate(baseUrl, tfundcode):
    tfundcompany = ''
    tfundsetup_date = ''

    target_url = baseUrl + 'jbgk_' + tfundcode + '.html'

    html = PYHTTPConnect.get_HTMLText(target_url)

    if html:
        bsobj = BeautifulSoup(html, 'html.parser')
        tfundinfo = bsobj.find('table', class_='info w790')

        if tfundinfo:
            bs_fundinfo = BeautifulSoup(str(tfundinfo), 'html.parser')

            bs_fundcompany = bs_fundinfo.find('a', href=re.compile('(http://fund.eastmoney.com/company/).*\.html'))
            if bs_fundcompany:
                tfundcompany = bs_fundcompany.text

            if bs_fundinfo.find('th', text='成立日期/规模'):
                bs_setupdate = bs_fundinfo.find('th', text='成立日期/规模').nextSibling
                if bs_setupdate:
                    if (len(str(bs_setupdate.text).split(' / ')) > 1):
                        tfundsetup_date = str(bs_setupdate.text).split(' / ')[0].strip('日').replace('年', '-').replace(
                            '月',
                            '-')
                    else:
                        tfundsetup_date = '0001-01-01'
    else:
        print("Unable to Get fundcompanyAndSetupDate From :" + target_url)

    if tfundsetup_date == '':
        tfundsetup_date = '0001-01-01'

    return tfundcompany, tfundsetup_date


'''
    更新基金信息表的记录（插入或更新）
'''


def insertORupdate_tfundinfo(tfundcode, tfundname, tfundtype):
    conn = PYDBConnect.mysqldbConnect()

    querycmd = " select * from TFUNDINFO where C_FUNDCODE = '" + tfundcode + "'"
    updatecmd = "update TFUNDINFO set C_FUNDNAME = '" + tfundname + "' , C_FUNDTYPE = '" + tfundtype + "' where C_FUNDCODE = '" + tfundcode + "'"
    insertcmd = "insert into TFUNDINFO (C_FUNDCODE, C_FUNDNAME, c_fundtype) values ('" + tfundcode + "', '" + tfundname + "', '" + tfundtype + "')"

    if conn:
        try:
            rowcount, rs = PYDBConnect.mysqldbQuery(conn, querycmd)
            if rowcount > 0:
                for row in rs:
                    if (tfundname != str(row[2])) or (tfundtype != str(row[3])):
                        PYDBConnect.mysqldbInsertDeleteUpdate(conn, updatecmd)
                        print("Update TFUNDINFO for C_FUNDCODE = " + tfundcode)
                    else:
                        print("TFUNDINFO exists record for C_FUNDCODE = " + tfundcode)
            else:
                PYDBConnect.mysqldbInsertDeleteUpdate(conn, insertcmd)
                print("Insert TFUNDINFO for C_FUNDCODE = " + tfundcode)
        except Exception as e:
            print("ERROR---insertORupdate_tfundinfo: {0} ".format(str(e)))
        finally:
            PYDBConnect.mysqldbConnClose(conn)


'''
    更新TFUNDINFO中基金管理人、基金成立日期等信息
'''


def update_fundcompanyAndsetupDate(tfundcode, tfundcomany, spdate):
    conn = PYDBConnect.mysqldbConnect()

    updatecmd = "update TFUNDINFO set C_FUNDCOMPANY = '" + tfundcomany + "', D_SETUPDATE = '" + spdate + "' where C_FUNDCODE = '" + tfundcode + "'"

    if conn:
        try:
            PYDBConnect.mysqldbInsertDeleteUpdate(conn, updatecmd)
            print("Update TFUNDINFO for C_FUNDCODE = " + tfundcode + " with " + tfundcomany + " and " + spdate)
        except Exception as e:
            print("Failed to run sql: " + updatecmd)
            print("ERROR---updateFundCompanyAndSetupDate: {0}".format(str(e)))
        finally:
            PYDBConnect.mysqldbConnClose(conn)


'''
    找出TFUNDINFO中基金管理人为空或基金成立日期为空的记录
'''


def select_nullfundcompanyAndsetupDate():
    rowcount = 0
    rs = {}

    conn = PYDBConnect.mysqldbConnect()

    querycmd = "select C_FUNDCODE from TFUNDINFO where C_FUNDCOMPANY is null or D_SETUPDATE is null"

    if conn:
        try:
            rowcount, rs = PYDBConnect.mysqldbQuery(conn, querycmd)
        except Exception as e:
            print("Failed to run sql: " + querycmd)
            print("ERROR---selectNullCompanyAndSPdate: {0}".format(str(e)))
        finally:
            PYDBConnect.mysqldbConnClose(conn)

    return rowcount, rs



'''
    清洗TFUNDINFO所有无效数据
'''


def clean_tfundinfo():
    rowcount = 0
    rs = {}

    conn = PYDBConnect.mysqldbConnect()

    if conn:
        try:
            querycmd = "select C_FUNDCODE from TFUNDINFO where C_FUNDCOMPANY = ''"
            rowcount, rs = PYDBConnect.mysqldbQuery(conn, querycmd)
            if rowcount > 0:
                for row in rs:
                    try:
                        updatecmd = "update TFUNDINFO set C_FUNDCOMPANY = null where C_FUNDCODE = '" + str(row[0]) + "'"
                        PYDBConnect.mysqldbInsertDeleteUpdate(conn, updatecmd)
                    except Exception as e:
                        print("Failed to run sql: " + updatecmd)
                        continue

            querycmd = "select C_FUNDCODE from TFUNDINFO where D_SETUPDATE ='0001-01-01'"
            rowcount, rs = PYDBConnect.mysqldbQuery(conn, querycmd)
            if rowcount > 0:
                for row in rs:
                    try:
                        updatecmd = "update TFUNDINFO set D_SETUPDATE = null where C_FUNDCODE = '" + str(row[0]) + "'"
                        PYDBConnect.mysqldbInsertDeleteUpdate(conn, updatecmd)
                    except Exception as e:
                        print("Failed to run sql: " + updatecmd)
                        continue

        except Exception as e:
            print("Failed to run sql: " + querycmd)
            print("ERROR---clean_tfundinfo: {0}".format(str(e)))
        finally:
            PYDBConnect.mysqldbConnClose(conn)


if __name__ == "__main__":

    # 更新基金基本信息：基金代码、基金名称、基金类型等
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": Start to scrapy TFUNDINFO...")
    fundinfo_dict = get_fundinfoList(target_fundlistUrl)

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
    rownum, resCol = select_nullfundcompanyAndsetupDate()
    if rownum > 0:
        for rs in resCol:
            ofundcode = str(rs[0])
            ofundcompany, ofundsetup_date = get_fundcompanyAndSetupDate(target_baseUrl, ofundcode)
            update_fundcompanyAndsetupDate(ofundcode, ofundcompany, ofundsetup_date)
            time.sleep(0.5)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": Update TFUNDINFO with fundcompany, setupdate Done!")

    # 清洗TFUNDINFO中的无效数据
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": Start to clean data in TFUNDINFO...")
    clean_tfundinfo()
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": Clean data in TFUNDINFO Done!")
