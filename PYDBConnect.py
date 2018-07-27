import pymysql

pydbhost = 'a.b.c.d'
pydbport = 1234
pydbusername = 'abcd'
pydbpassword = 'abcd1234'
pydbname = 'db1'
pydbcharset = 'utf8'

'''
    mysql的连接函数
'''


def mysqldbConnect():
    conn = None
    try:
        conn = pymysql.connect(host=pydbhost,
                               user=pydbusername,
                               password=pydbpassword,
                               db=pydbname,
                               charset=pydbcharset,
                               port=pydbport)
        conn.autocommit(False)
    except Exception as e:
        print("Failed to Connect MYSQL DB: " + str(e))

    return conn


'''
    mysql的查询语句执行
'''


def mysqldbQuery(conn, sqlcmd):
    rowcount = 0
    rs = {}
    cursor = conn.cursor()
    try:
        cursor.execute(sqlcmd)
        rowcount = cursor.rowcount
        rs = cursor.fetchall()
    except Exception as e:
        print("Failed to run sql: " + sqlcmd)
        print(str(e))
    finally:
        cursor.close()
    return rowcount, rs


'''
    mysql的insert, delete, update执行
'''


def mysqldbInsertDeleteUpdate(conn, sqlcmd):
    cursor = conn.cursor()
    try:
        cursor.execute(sqlcmd)
        if (cursor.rowcount != 1):
            raise Exception("Cursor rowcount = {0}".format(rowcount))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Failed to run sql: " + sqlcmd)
        print(str(e))
    finally:
        cursor.close()


'''
    关闭mysql的连接
'''


def mysqldbConnClose(conn):
    if (conn):
        conn.close()


def select_allfundcodes():
    rowcount = 0
    rs = {}

    conn = mysqldbConnect()
    querycmd = "select C_FUNDCODE from TFUNDINFO"

    if conn:
        try:
            rowcount, rs = mysqldbQuery(conn, querycmd)
        except Exception as e:
            print("Failed to run sql: " + querycmd)
            print("ERROR---selectAllFundcode：{0}".format(str(e)))
        finally:
            mysqldbConnClose(conn)

    return rowcount, rs


if __name__ == "__main__":

    connetion = mysqldbConnect()
    sqlcmd = "select * from TFUNDINFO where C_FUNDCODE =000001"

    try:
        rowcount, rs = mysqldbQuery(connetion, sqlcmd)
        if (rowcount >= 1):
            for row in rs:
                print(row)
    except Exception as e:
        print("mysqldbQuery报错: {0}".format(str(e)))
    finally:
        mysqldbConnClose(connetion)
