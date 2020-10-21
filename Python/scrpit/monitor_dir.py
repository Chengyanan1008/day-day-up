#!/usr/bin/env python3.6
import os
import datetime
import pyinotify
import logging
import smtplib
import time
import threading
from email.mime.text import MIMEText
from email.utils import formataddr
flag=1
modifytime = time.time()


class MyEventHandler(pyinotify.ProcessEvent):
    logging.basicConfig(level=logging.INFO,filename='/home/kaixin/monitor.log')
    # 自定义写入那个文件，可以自己修改
    print("Starting monitor...")
    def process_IN_ACCESS(self, event):
        print ("ACCESS event:", event.pathname)
        logging.info("ACCESS event : %s  %s" % (os.path.join(event.path,event.name),datetime.datetime.now()))
    def process_IN_ATTRIB(self, event):
        print ("ATTRIB event:", event.pathname)
        logging.info("IN_ATTRIB event : %s  %s" % (os.path.join(event.path,event.name),datetime.datetime.now()))
    def process_IN_CLOSE_NOWRITE(self, event):
        print ("CLOSE_NOWRITE event:", event.pathname)
        logging.info("CLOSE_NOWRITE event : %s  %s" % (os.path.join(event.path,event.name),datetime.datetime.now()))
# 
    def process_IN_CLOSE_WRITE(self, event):
        print ("CLOSE_WRITE event:", event.pathname)
        logging.info("CLOSE_WRITE event : %s  %s" % (os.path.join(event.path,event.name),datetime.datetime.now()))

    def process_IN_CREATE(self, event):
        print ("CREATE event:", event.pathname)
        logging.info("CREATE event : %s  %s" % (os.path.join(event.path,event.name),datetime.datetime.now()))

    def process_IN_DELETE(self, event):
        print ("DELETE event:", event.pathname)
        logging.info("DELETE event : %s  %s" % (os.path.join(event.path,event.name),datetime.datetime.now()))

    def process_IN_MODIFY(self, event):
        global modifytime
        print ("MODIFY event:", event.pathname)
        modifytime = time.time()
        logging.info("MODIFY event : %s  %s" % (os.path.join(event.path,event.name),datetime.datetime.now()))

    def process_IN_OPEN(self, event):
        print ("OPEN event:", event.pathname)
        logging.info("OPEN event : %s  %s" % (os.path.join(event.path,event.name),datetime.datetime.now()))


def notify():
    # watch manager
    wm = pyinotify.WatchManager()
    wm.add_watch('/data/BDP/analytics/logs', pyinotify.ALL_EVENTS, rec=True)
    # wm.add_watch('/home/kaixin', pyinotify.ALL_EVENTS, rec=True)
    # /tmp是可以自己修改的监控的目录
    # event handler
    eh = MyEventHandler()

    # notifier
    notifier = pyinotify.Notifier(wm, eh)
    notifier.loop()


def checktime():
    while (True):
        global modifytime
        global flag
        time.sleep(10)  # 每10秒去监控一次时间
        thistime = time.time()
        thisHour = time.strftime("%H", time.localtime())
        if ( thisHour == '10'):
            flag=1
        if(thisHour=='09'):
            if flag:
                mail2dengyun(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(modifytime)))
                flag=0
        if (thistime - modifytime > 500):
            mail2u(str(thistime - modifytime))


def main():
    t1 = threading.Thread(target=notify)
    t1.start()
    t2 = threading.Thread(target=checktime)
    t2.start()

def mail2dengyun(stime):
    # 第三方 SMTP 服务
    mail_host = "smtp.idengyun.com"  # 设置服务器
    mail_user = "chengyanan@idengyun.com"  # 用户名
    mail_pass = "dengyun123456"  # 口令
    sender = 'chengyanan@idengyun.com'
    receivers = 'chengyanan@idengyun.com'  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
    message = MIMEText('监控程序功能正常,最近一次数据同步的时间为：'+ stime, 'plain', 'utf-8')
    message['From'] = formataddr(["dengyun日常提醒邮件", sender])
    message['To'] = formataddr(("dengyun", receivers))
    # 主题
    subject = '10.13.0.22服务器日常提醒邮件'
    message['Subject'] = subject
    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")

def mail2u(msg):
    # 第三方 SMTP 服务
    mail_host = "smtp.idengyun.com"  # 设置服务器
    mail_user = "chengyanan@idengyun.com"  # 用户名
    mail_pass = "dengyun123456"  # 口令
    sender = 'chengyanan@idengyun.com'
    receivers = 'chengyanan@idengyun.com'  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
    message = MIMEText('程序停止，请及时检查！上次同步时间为' + msg + "秒前", 'plain', 'utf-8')
    message['From'] = formataddr(["dengyun", sender])
    message['To'] = formataddr(("dengyun", receivers))
    # 主题
    subject = '10.13.0.22服务器数据同步故障提醒邮件'
    message['Subject'] = subject
    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")


if __name__ == '__main__':
    main()
