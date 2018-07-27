import time
import re

from bs4 import BeautifulSoup

import PYDBConnect
import PYHTTPConnect


target_baseJS = "http://fund.eastmoney.com/f10/FundArchivesDatas.aspx"

'''
    访问URL，获取基金的十大重仓股，返回股票代码、股票名称、占基金净值比例、持股数（万股）、持仓市值（万元）、报告日期
'''


def get_fundstockinfo(baseUrl, tfundcode):
    stockcode = ''
    stockname = ''
    assetratio = ''
    stockquantity = ''
    stockvalue = ''
    reportdate = ''

    res_col = []

    target_url = baseUrl + "?type=jjcc&code=" + tfundcode + "&topline=10"

    html = PYHTTPConnect.get_HTMLText(target_url)
    if html:
        bsobj = BeautifulSoup(html, 'html.parser')
        # print(bsobj.prettify())
        if bsobj:
            tfundstockinfo_col = bsobj.find_all('div', class_='boxitem w790')
            if len(tfundstockinfo_col) > 0:
                for tfundstockinfo in tfundstockinfo_col:
                    bs_fundstockinfo = BeautifulSoup(str(tfundstockinfo), 'html.parser')
                    bs_reportdate = bs_fundstockinfo.find('font', class_='px12')
                    if bs_reportdate:
                        reportdate = bs_reportdate.text

                    tfundstocktable = bs_fundstockinfo.find('table', class_='w782 comm tzxq')
                    if tfundstocktable:
                        bs_fundstocktable = BeautifulSoup(str(tfundstocktable), 'html.parser')
                        if bs_fundstocktable.tbody:
                            bs_fundstockrow_col = BeautifulSoup(str(bs_fundstocktable.tbody), 'html.parser').findAll(
                                'tr')
                            if len(bs_fundstockrow_col) > 0:
                                for bs_fundstockrow in bs_fundstockrow_col:
                                    bs_stocktd_col = BeautifulSoup(str(bs_fundstockrow), 'html.parser').findAll('td')
                                    if len(bs_stocktd_col) == 9:
                                        stockcode = bs_stocktd_col[1].text
                                        stockname = bs_stocktd_col[2].text
                                        assetratio = bs_stocktd_col[6].text
                                        stockquantity = bs_stocktd_col[7].text
                                        stockvalue = bs_stocktd_col[8].text
                                        res_col.append(
                                            [tfundcode, reportdate, stockcode, stockname, assetratio, stockquantity,
                                             stockvalue])

                                    if len(bs_stocktd_col) == 7:
                                        stockcode = bs_stocktd_col[1].text
                                        stockname = bs_stocktd_col[2].text
                                        assetratio = bs_stocktd_col[4].text
                                        stockquantity = bs_stocktd_col[5].text
                                        stockvalue = bs_stocktd_col[6].text
                                        res_col.append(
                                            [tfundcode, reportdate, stockcode, stockname, assetratio, stockquantity,
                                             stockvalue])

    else:
        print("Unable to Get get_fundManagersAndAssetAndReportdate From :" + target_url)

    return res_col


'''
    插入数据库中的基金股票持仓表（插入或者更新）
'''


def insert_tfundstock(tfundcode, treportdate, tstockcode, tstockname, tassetration, tstocknum, tstockval):
    conn = PYDBConnect.mysqldbConnect()

    querycmd = "select C_FUNDCODE from TFUNDSTOCK where C_FUNDCODE = '" + tfundcode + "' and D_REPORTDATE = '" + treportdate + "' and C_STOCKCODE = '" + tstockcode + "'"
    insertcmd = "insert into TFUNDSTOCK (C_FUNDCODE, D_REPORTDATE, C_STOCKCODE, C_STOCKNAME, C_ASSETRATION, C_STOCKQUANTITY, C_STOCKVALUE)  \
                                values ('" + tfundcode + "', '" + treportdate + "', '" + tstockcode + "', '" + tstockname + "', '" + tassetration + "', '" + tstocknum + "', '" + tstockval + "')"

    try:
        rowcount, rs = PYDBConnect.mysqldbQuery(conn, querycmd)
        if (rowcount == 0):
            PYDBConnect.mysqldbInsertDeleteUpdate(conn, insertcmd)
            print(
                "Insert TFUNDSTOCK for fundcode = " + tfundcode + ", reportdate = " + treportdate + ", stockcode = " + tstockcode)
        else:
            print(
                "TFUNDSTOCK exist record for fundcode = " + tfundcode + ", reportdate = " + treportdate + ", stockcode = " + tstockcode)
    except Exception as e:
        print(
            "Failed to update TFUNDSTOCK with fundcode = " + tfundcode + ", reportdate = " + treportdate + ", stockcode = " + tstockcode)
        print("ERROR---insert_tfundreport: {0}".format(str(e)))
    finally:
        PYDBConnect.mysqldbConnClose(conn)




if __name__ == '__main__':

    # 更新基金持仓表：基金代码、基金报告期、股票代码、股票名称、持仓占净值比例、股票数量、股票市值
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": Start to scrapy TFUNDSTOCK...")
    row_num, res_Col = PYDBConnect.select_allfundcodes()
    if (row_num > 0):
        for res in res_Col:
            time.sleep(0.5)
            o_fundcode = str(res[0])
            res_Col = get_fundstockinfo(target_baseJS, o_fundcode)

            if len(res_Col) > 0:
                for res in res_Col:
                    o_reportdate = res[1]
                    o_stockcode = res[2]
                    o_stockname = res[3]
                    o_assetration = res[4]
                    o_stocknum = res[5]
                    o_stockval = res[6]
                    insert_tfundstock(o_fundcode, o_reportdate, o_stockcode, o_stockname, o_assetration, o_stocknum, o_stockval)

    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": Update scrapy TFUNDSTOCK Done!")