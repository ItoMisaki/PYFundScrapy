import socket
import time
from urllib import request

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

'''
    输入URL，使用代理与请求头，返回报文
'''


def get_HTMLText(url):
    response = False
    html = None

    proxy_support = request.ProxyHandler(target_proxy)
    opener = request.build_opener(proxy_support)
    opener.addheaders = target_headers
    request.install_opener(opener)

    try:
        response = request.urlopen(url)
        html = response.read().decode("utf-8")
    except Exception as e:
        print("Failed to open Url: " + url)
        print(str(e))
        print("Try to open Url again : " + url)
        try:
            time.sleep(1)
            response = request.urlopen(url)
            html = response.read().decode("utf-8")
        except Exception as e:
            print("Fail to open Url: " + url)
            print(str(e))
            print("End trying to open it")
    finally:
        opener.close()
        response.close()

    return html
