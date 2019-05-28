#!/usr/bin/env python3
# -*-coding:utf-8-*-

# author : seale

import os
import sys
import subprocess
import time
import random
import datetime
import json
import traceback
import pymysql #add by simple

from db.model import db, Prices_tb
from spider.detailSpider import ItemDetail_selenium
from utils.logger   import Log
from utils.sendmail import Sendmail
from config import path, shops, headless, item_num, item_time
from random import choice



proxy_list=[
'219.131.191.168:4223',
'27.40.154.164:4258'
]


#系统测试
Test=False #True #False
 
'''print('输入最大数据：')
a = input()
if( (a == 'q') or (a == 'Q') ):
    exit
max=int(a)'''


Gstart=datetime.datetime.now()

 
with open(os.path.join(path, 'userCookie.txt'), 'r') as f:
    cookies_str = json.loads(f.read())

'''if len(sys.argv) >= 2:
删除此段

else:'''
Step=1
while True:
    for shop in shops:
        log_path = os.path.join(path, 'log', shop['url_name'])
        if not os.path.exists(log_path):
            os.makedirs(log_path)
        nowdate = time.strftime("%Y-%m-%d",time.localtime(time.time())) 
        logfile=shop['name'] + nowdate
        logger = Log(logfile, log_path, '%(asctime)s - %(message)s')
        if Step>3:
            Step=1
            
        if(Step==1):
            #长期没有更新的数据
            sql="select link_id,SpiderDate,stockid from prices_tb where shop_id='%s' ORDER BY SpiderDate ASC limit 100" % (shop['shopid'])
            logger.info("Step 1:更新时间按升序排列，处理前面的数据:")
            Step=Step+1
                
        else:
            if(Step==2):
                #处理超过一周没有更新的数据
                sql="select link_id,SpiderDate,stockid from prices_tb where shop_id='%s' and DATE_SUB(CURDATE(), INTERVAL 7 DAY) >= date(SpiderDate) ORDER BY SpiderDate ASC limit 300" % (shop['shopid'])
                logger.info("Step 2:处理大于7天没有更新的数据:")
                Step=Step+1
            else:
                if(Step==3):
                    sql="select link_id,SpiderDate,stockid from prices_tb where shop_id='%s'  ORDER BY last_time DEC limit 100" % (shop['shopid'])
                    logger.info("Step 3:处理人工修改时间的数据:")
                    Step=Step+1
            
        try:
            conn=pymysql.connect("www.shai3c.com",port=3306,user="remote",passwd="sz654321",db="weberp",charset='utf8');
            with conn.cursor() as cursor:
                cursor.execute(sql)
                results=cursor.fetchall()
                #没有设置默认自动提交，需要主动提交，以保存所执行的语句
                conn.commit()
        except Exception as e:
                # 错误回滚
                conn.rollback()
                print(sql)
                print(repr(e))
        finally:
                conn.close()  #释放数据库资源  
        
        #构建需要处理的数据  
        ids=set() 
        MyCounter=0 #计数器
        for row in results:
            ids.add(row[0]) 
            #显示出10个满足条件的数据
            if(MyCounter<10):
                logger.info("代码:" +str(row[2]) )   #logger.info("最早数据的日期:")
                logger.info("链接:https://item.taobao.com/item.htm?&id=" +str(row[0]))
                logger.info("日期:"+ str(row[1]) )
                logger.info("")
                MyCounter=MyCounter+1
        #构建需要处理的数据    

        
        if ids == None or len(ids) == 0:
            logger.info('**************************************************')
            logger.info('*          ' + shop['name'] + '已经爬完了        *')
            logger.info('**************************************************')
            continue
        else:
            print(ids)#显示全部需要处理的id
         
        #打开已经爬取的文件            
        crawled_file = os.path.join(log_path, 'crawled.json')
        if os.path.exists(crawled_file):
            with open(crawled_file, 'r', encoding='utf-8') as f:
                string = f.read()
                if string == None:
                    crawled_ids = set()
                else:
                    crawled_ids = set(json.loads(string))
        else:
            crawled_ids = set()
        
        '''db.connect(reuse_if_open=True)
        remote_ids = Prices_tb.beforeDeal(db, shop['shopid'])
        if remote_ids == 0 or remote_ids == None:
            ids = set(ids)
        else:
            ids = set(ids)
            ids = ids | remote_ids'''
            
        ids = ids - crawled_ids
        num=len(ids)
        print("Total Num:%s" % (num))
        
        #随机抽取代理
        proxy=choice(proxy_list)
        print("proxy：%s" % (proxy))
        
        detailspider = ItemDetail_selenium(logger, cookies_str, headless,None)
        detailspider.getBrowser()
        detailspider.login()
        
        datas = []
        xiajia_list = []
              
        if item_num > len(ids) or item_num == 0:
            item_num = len(ids)
        loop_times = 1

       
        try:
            while item_num:  #循环抓取数据
                print("")
                print("剩余数量：%s" % (item_num))
              
                if loop_times % item_time == 0:
                    detailspider.close()
                    detailspider.getBrowser()
                    detailspider.login()
                    logger.info('重启浏览器')
                else:
                    detailspider.change_cookies()
                    #logger.info('更换cookies')
                
                #全部爬完，退出循环
                if len(ids) == 0:
                    break
                
                #继续
                id = ids.pop()
        
                start = time.time()
                print(time.strftime("%Y-%m-%d %H:%M:%S")) #带日期的24小时格式
                if(Test):
                    print("几点钟："+time.strftime("%H"))
                print('爬取商品ID为：' + id)
                
                tt=random.randint(3,10)
                try:
                    data = detailspider.fetch(id, shop['shopid'])
                except Exception as e:
                    print(repr(e))
                    print("detailspider.fetch() 异常")
                    data = None
                    # ids.add(id)
                    time.sleep(tt)

                if data == 0:
                    print('已下架')
                    xiajia_list.append(id)
                    crawled_ids.add(id) #后面这句模仿正常抓到数据
                    
                    #start 把下架的数据，直接跟新至数据中，避免退出时的反应怠速 2019-1-31
                    now = str(datetime.datetime.now()).split('.')[0]
                    sql_update="update prices_tb set flag='XiaJia',SpiderDate='%s' where link_id='%s' "

                    try:
                        mydb=pymysql.connect("www.shai3c.com",port=3306,user="remote",passwd="sz654321",db="weberp",charset='utf8');
                        with mydb.cursor() as cur:
                            cur.execute(sql_update % (now,id))  #像sql语句传递参数
                            mydb.commit()#提交
                        
                    except Exception as e:
                        #错误回滚
                        mydb.rollback() 
                        print(id + '' +now )

                    finally:
                        mydb.close()#释放数据库资源    
                    #end
                    
                    item_num -= 1
                    loop_times += 1
                    end=time.time()
                    print("本次用时:" +str(end- start))#本次花费时间
                                  
                    tmpdelay=random.randint(6,15)
                    time.sleep(tmpdelay + random.random()) #随机延迟
                
                    continue
                elif data == None:
                    print('获取数据异常！')
                    ids.add(id)
                    time.sleep(tt)
                    continue
                else:
                    print(data)
                    datas += data

                #logger.info('=====保存开始=====')
                MyCounter=len(data)
                if(MyCounter>1):
                    print("宝贝%s为MUT=%s" % (id,MyCounter))
                              
                #测试网络是否正常 start
                while 1:
                    '''if subprocess.Popen('ping www.baidu.com -n 3', stdout=subprocess.PIPE).wait():
                        time.sleep(300)
                        else:
                        break'''
                    #网络连通 exit_code == 0，否则返回非0值。    
                    exit_code = os.system('ping www.baidu.com')
                    if exit_code:
                        time.sleep(300)
                        #raise Exception('connect failed.')
                    else:
                        break
                #测试网络是否正常 end
                
                db.connect(reuse_if_open=True)
                
                logger.info('序号:'+str(item_num)+' ,开始')
                for elem in data:
                    #精确取出数据，如属性修改，就会有数据匹配不上
                    item = Prices_tb.select().where(Prices_tb.link_id == id, Prices_tb.attribute == elem['attribute'], Prices_tb.shop_id == shop['shopid'])
                    count=0                   
                    #dict[newkey] = dict.pop(key) #更改字典的key
                    elem['price_tb']  =elem.pop('price')
                    elem['link_id']   =elem.pop('linkid')
                    elem['shop_id']   =elem.pop('shopid')
                    elem['freight']   =elem.pop('kuaidi')
                    elem['SpiderDate'] =elem.pop('time')
                    
                                                              
                    if len(item) == 0:
                        logger.info('此数据在erp里还没有找到:https://item.taobao.com/item.htm?&id=' + id +' 属性:' + elem['attribute'] )
                        continue
                    else:
                        for i in item:  #数据库里有该数
                            elem['stockid'] = i.stockid
                            
                            #第一次输出详细信息
                            if(count==0):
                                if( elem['price_tb'] != i.price_tb or elem['promotionprice'] != i.promotionprice or elem['description'] != i.description):
                                    logger.info('代码:'+ i.stockid )
                                    if(i.description):
                                        logger.info('名称:'+str(i.description))
                                    logger.info('连接:https://item.taobao.com/item.htm?&id=' + id)
                                    
                                    #发现价格或名称有变化，        
                                    if( elem['price_tb'] != i.price_tb or elem['promotionprice'] != i.promotionprice):
                                        if(elem['attribute']):
                                            logger.info('属  性:'+str(elem['attribute']))
                                        logger.info('旧价格:'+str(i.price_tb )+':'+str(i.promotionprice))
                                        logger.info('新价格:'+str(elem['price_tb'])+':'+str(elem['promotionprice']))
                                
                                    if( elem['description'] != i.description ): 
                                        logger.info( '新名称:' + elem['description'])
                                        logger.info( '旧名称:' + i.description)
                            else:
                                #多属性输出其他属性的价格
                                if( elem['price_tb'] != i.price_tb or elem['promotionprice'] != i.promotionprice):
                                    if(elem['attribute']):
                                        logger.info('属  性:'+str(elem['attribute']))
                                    logger.info('旧价格:'+str(i.price_tb )+':'+str(i.promotionprice))
                                    logger.info('新价格:'+str(elem['price_tb'])+':'+str(elem['promotionprice']))

                            if (i.price_tb == 0):#防止价格为0,add by Simple Li
                                i.price_tb=1#continue
                            ratio = elem['price_tb'] / i.price_tb
                            now = str(datetime.datetime.now()).split('.')[0]
                            Prices_tb.update(description=elem['description'],
                                             price_tb=elem['price_tb'], 
                                             promotionprice=elem['promotionprice'], 
                                             flag='update', 
                                             ratio=ratio,
                                             sales=elem['sales'],
                                             rates=elem['rates'], 
                                             SpiderDate=elem['SpiderDate']).where(Prices_tb.link_id == id, Prices_tb.attribute == elem['attribute']).execute()
                            count=count+1
                        #end :for i in item
                        
                    #end of else
                #end: for elem in data:
                logger.info(str(item_num) + ':结束')
                logger.info('')
                logger.info('')
                 
                db.close()#add by simple 2018-12-21,目的想避免db连接超时
                crawled_ids.add(id) #记录已爬ID
                                
                item_num -= 1
                loop_times += 1
                
                #白天、黑夜采用不同的延迟
                Hour=int(time.strftime("%H"))
                if(Hour>8 and Hour <23):
                    tmpdelay=random.randint(8,30)
                else:
                    tmpdelay=random.randint(15,60)
                    
                end=time.time()
                print("本次用时:" +str(end- start))#本次花费时间
                
                print('延迟时间:',tmpdelay)
                time.sleep(tmpdelay + random.random()) #随机延迟
                #end of for
            #end of while    
        except Exception as e:
            logger.info('--------------------------------------------')
            logger.info('出错')
            logger.info(traceback.print_exc())
            logger.info('--------------------------------------------')
            
           
            detailspider.close()#异常退出释放资源linux 运行发现的问题    
            
            logger.info('停止时间：' + str(datetime.datetime.now()))
        finally:
            with open(crawled_file, 'w', encoding='utf-8') as f:
                print("记录已爬id，写入crawled.json文件")
                f.write(json.dumps(list(crawled_ids)))
            detailspider.close()
            
            print('当前任务退出，关闭浏览器')
            
            logger.info('')
            logger.info('爬取结束时间：' + str(datetime.datetime.now()))
            logger.info('已爬商品数量：' + str(len(crawled_ids)))
            logger.info('待爬商品数量：' + str(len(ids)))
            
       
            if(len(xiajia_list)>0):
                logger.info('已下架:')
                
                for line in xiajia_list:
                    #logger.info(str(line))
                    item = Prices_tb.select().where(Prices_tb.link_id == line,Prices_tb.shop_id == shop['shopid']).get()
                    logger.info('商品代码:' + item.stockid )
                    logger.info('商品连接:' +'https://item.taobao.com/item.htm?&id=' + str(line))  
                #end of for
                                   
            
            if len(ids) == 0:
                #全部爬完清空2个文件
                with open(os.path.join(log_path, 'success.json'), 'w', encoding='utf-8') as f:
                    f.write(json.dumps([]))
                with open(crawled_file, 'w', encoding='utf-8') as f:
                    f.write(json.dumps([]))
                             
                logger.info('**************************************************')
                logger.info('*             ' +   shop['name'] + '爬完了       *')
                logger.info('**************************************************')
                     

            
            #发送邮件
            receivers ='szjavali@qq.com'  # 接收邮件
            Mail=Sendmail()
            
            #开源  
            Libj  ='743679187@qq.com' #李博坚 
            Tangzh='79248458@qq.com'  #唐志鸿
            
            #玉佳
            Lizp ='82064297@qq.com'     #李战平
            Zhangqy='43762545@qq.com'  #张朝阳
            
            #赛宝
            Yangw ='82064297@qq.com'  #杨文
            Liangxj='2442879033@qq.com'#梁祥健

            Hezh='myzhiheng@163.com'


            if(Test):
                receivers ='szjavali@qq.com'#test
                Subject="这是一个测试，请不用理会！"
                Mail.set_subject(Subject)
            else:
                Subject=logfile
                Mail.set_subject(Subject)
                if(shop['name'] == '开源电子'): 
                    receivers=Libj     #赛宝：李博坚
                    Mail.set_cc(Tangzh)  #抄送：唐志鸿
                    Mail.set_bcc(Libj)
                if(shop['name'] == '育松电子'):
                    receivers =Tangzh    #优信：唐志鸿
                    Mail.set_cc(Caotj)  #抄送: 曹太军
                    Mail.set_bcc(Hezh)
                        
                if(shop['name'] == '玉佳电子'):
                    receivers=Zhangqy     #玉佳：张朝阳
                    Mail.set_cc(Lizj)     #抄送  李战平
                    Mail.set_bcc(Libj)
                if(shop['name'] == '信泰微'):
                    receivers =Lizj    #信泰微：李战平
                    Mail.set_cc(Zhangqy) #抄送：杨文
                    Mail.set_bcc(Hezh)
                
                if(shop['name'] == '赛宝电子'):
                    receivers=Liangxj    #开源：梁祥健
                    Mail.set_cc(Yangw)   #抄送  杨文
                    Mail.set_bcc(Libj)
                if(shop['name'] == '优信电子'):
                    receivers =Yangw    #育松：杨文
                    Mail.set_cc(Liangxj) #抄送 梁祥健 
                    Mail.set_bcc(Hezh)

            filename=logfile +'.log'
            if(Test):
                print(filename) 
            
            #白天发邮件
            Hour=int(time.strftime("%H"))
            if(Hour==10 or Hour ==16 or  Hour ==24):    
                Mail.add_attachment(log_path,filename)
                ret=Mail.send(receivers)
                if ret:
                    print("邮件发送成功")
                else:
                    print("邮件发送失败")
                
        #end of finally
      
        #程序运行时间
        Gend=datetime.datetime.now()
        print('Running time: %s Seconds'%(Gend-Gstart))
    
    #end of for(shop)
    Step=Step+1
#end of while    