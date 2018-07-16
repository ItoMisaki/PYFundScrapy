import pymysql

''
pydbhost = 'a.b.c.d'
pydbport = 1234
pydbusername = 'abcd'
pydbpassword = 'abcd1234'
pydbname = 'db1'

'''
    mysql的连接函数
'''
def mysqldbConnect(username, passwd, db, host, dbport, charset='utf8'):
    conn = pymysql.connect(host=host,
                           user=username,
                           password=passwd,
                           db=db,
                           charset=charset,
                           port=dbport)
    conn.autocommit(False)
    return conn


'''
    mysql的查询语句执行
'''
def mysqldbQuery(conn, sqlcmd):
    cursor = conn.cursor()
    try:
        cursor.execute(sqlcmd)
        rowcount = cursor.rowcount
        rs = cursor.fetchall()
    except Exception as e:
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
            raise Exception("执行SQL语句失败：{0}".format(sqlcmd))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(str(e))
    finally:
        cursor.close()


'''
    关闭mysql的连接
'''
def mysqldbConnClose(conn):
    if (conn):
        conn.close()


if __name__ == "__main__":

    connetion = mysqldbConnect(pydbusername, pydbpassword, pydbname, pydbhost, pydbport)
    sqlcmd = "select * from TFUNDINFO"

    try:
        rowcount, rs = mysqldbQuery(connetion, sqlcmd)
        if (rowcount >= 1):
            for row in rs:
                print(row)
    except Exception as e:
        print("mysqldbQuery报错：{0}".format(str(e)))
    finally:
        mysqldbConnClose(connetion)
