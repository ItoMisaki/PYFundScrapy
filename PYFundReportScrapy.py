import time
import re

from bs4 import BeautifulSoup

import PYDBConnect
import PYHTTPConnect

target_baseUrl = 'http://fund.eastmoney.com/f10/'


'''
    访问URL，获取报文，获取基金经理、基金规模、最新报告日期等信息
'''


def get_fundManagersAndAssetAndReportdate(baseUrl, tfundcode):
    tfundmanager = ''
    tfundasset = ''
    tfundreport_date = ''

    target_url = baseUrl + 'jbgk_' + tfundcode + '.html'

    html = PYHTTPConnect.get_HTMLText(target_url)

    if html:
        bsobj = BeautifulSoup(html, 'html.parser')
        tfundinfo = bsobj.find('table', class_='info w790')
        if tfundinfo:
            bs_fundinfo = BeautifulSoup(str(tfundinfo), 'html.parser')

            bs_fundmanagers = bs_fundinfo.find_all('a', href=re.compile('(http://fund.eastmoney.com/manager/).*\.html'))
            if len(bs_fundmanagers) > 0:
                for bs_fundmanager in bs_fundmanagers:
                    tfundmanager += (bs_fundmanager.text + "|")

            if bs_fundinfo.find('th', text='资产规模'):
                bs_fundassetrepdate = bs_fundinfo.find('th', text='资产规模').nextSibling
                if bs_fundassetrepdate:
                    tfundassetrepdate = str(bs_fundassetrepdate.text).split('份额规模')[0].strip()

                    if (len(str(tfundassetrepdate).split('亿元（截止至：')) > 1):
                        tfundasset = str(tfundassetrepdate).split('亿元（截止至：')[0]
                        tfundreport_date = str(tfundassetrepdate).split('亿元（截止至：')[1].strip('日）').replace('年',
                                                                                                          '-').replace(
                            '月',
                            '-')
                    else:
                        tfundasset = '0.00'
                        tfundreport_date = '0001-01-01'
    else:
        print("Unable to Get get_fundManagersAndAssetAndReportdate From :" + target_url)

    return tfundmanager, tfundasset, tfundreport_date


'''
    插入数据库中的基金报告表（插入或更新）
'''


def insert_tfundreport(tfundcode, treportdate, tfundmanager, tfundasset):
    conn = PYDBConnect.mysqldbConnect()

    querycmd = "select * from TFUNDREPORT where C_FUNDCODE = '" + tfundcode + "' and D_REPORTDATE = '" + treportdate + "'"
    insertcmd = "insert into TFUNDREPORT (C_FUNDCODE, D_REPORTDATE, C_FUNDMANAGER, C_FUNDASSET) \
                                  values ('" + tfundcode + "', '" + treportdate + "', '" + tfundmanager + "', '" + tfundasset + "')"

    try:
        rowcount, rs = PYDBConnect.mysqldbQuery(conn, querycmd)
        if (rowcount == 0):
            PYDBConnect.mysqldbInsertDeleteUpdate(conn, insertcmd)
            print("Insert TFUNDREPORT for fundcode = " + tfundcode + ", reportdate = " + treportdate)
        else:
            print("TFUNDREPORT exists record for c_fundcode = " + tfundcode + " and d_reportdate = " + treportdate)
    except Exception as e:
        print("Failed to update TFUNDREPORT with fundcode = " + tfundcode + ", reportdate = " + treportdate)
        print("ERROR---insert_tfundreport: {0}".format(str(e)))
    finally:
        PYDBConnect.mysqldbConnClose(conn)


if __name__ == "__main__":

    # 更新基金报告表：基金代码、基金报告期、基金经理、基金规模
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": Start to scrapy TFUNDREPORT...")
    row_num, res_Col = PYDBConnect.select_allfundcodes()
    if (row_num > 0):
        for res in res_Col:
            o_fundcode = str(res[0])
            o_fundmanager, o_fundasset, o_fundreportdate = get_fundManagersAndAssetAndReportdate(
                target_baseUrl,
                o_fundcode)
            insert_tfundreport(o_fundcode, o_fundreportdate, o_fundmanager, o_fundasset)
            time.sleep(0.5)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": Update TFUNDREPORT Done!")
