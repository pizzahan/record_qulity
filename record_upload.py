# -*- coding: utf-8 -*-
import argparse
import datetime
import logging.config
import os
import signal
import time
import traceback

import record_config
import threading


class Record(object):
    def __init__(self, config):
        self.configer = record_config.AsrConfig(config)

    # 上传文件
    def upload(self, destFile, srcFile):
        return self.configer.bucket.put_object_from_file(destFile, srcFile)

    # 删除文件
    def delete(self, destFile):
        return self.configer.bucket.delete_object(destFile)

    # 也可以批量删除
    def batchDelete(self, destFiles):
        return self.configer.bucket.batch_delete_objects(destFiles)

    # 检查文件是否存在
    def checkExists(self, destFile):
        return self.configer.bucket.object_exists(destFile)


def process(configer, beginTime, endTime):
    logging.info("process file between {0} and {1} ".format(beginTime, endTime)) # 获取待上传文件
    srcDb, err = configer.get_src_db()
    if err is not None:
        logging.error(err)
        return
    destDb, err = configer.get_dest_db()
    if err is not None:
        logging.error(err)
        return
    try:
        icnt = 0
        srcCur = srcDb.cursor()
        destCur = destDb.cursor()
        # 呼叫结果配置 pre_cc_biztemp.result 和业务关系比较复杂，后面优化
        # resultSql = ''' select cid,result from pre_cc_biztemp '''
        # effect_rows = srcCur.execute(resultSql)
        # if effect_rows > 0:
        #     rows = srcCur.fetchall()
        #     for row in rows:
        #         cid = row[0]
        #         result = row[1]

        # srcSql1 = ''' select a.dphone,c.dirid,c.filename,a.id,a.cid,a.prjid,b.duration,d.memo,e.memo,a.content,a.uid,f.name workerno,f.memo workername,a.dateline from pre_cc_biztrack a,pre_cc_call b,pre_cc_recfile c,pre_cc_corp d,pre_cc_project e, pre_cc_user f
        #         where a.result=2 and a.callid = b.id and b.recfileid = c.id and a.cid = d.id and a.prjid = e.id and a.uid = f.id and a.dateline BETWEEN {0} and {1}'''.format(
        #     beginTime, endTime)
        # mobile = row[0]
        # recDir = row[1]
        # fileName = row[2]
        # srcId = row[3]
        # cid = row[4]
        # prjid = row[5]
        # duration = row[6]
        # company_name = row[7]
        # project_name = row[8]
        # content = row[9]
        # user_id = row[10]
        # worker_id = row[11]
        # worker_name = row[12]
        # dateline = row[13]

        srcSql = ''' select a.id, a.dphone, a.callid, a.cid, a.prjid, a.content, a.uid, a.dateline, a.status, a.chkmemo  from pre_cc_biztrack a
                where a.result=2 and  prjid not in (381,382,384) and a.dateline BETWEEN {0} and {1}'''.format(
            beginTime, endTime)
        effect_rows = srcCur.execute(srcSql)
        if effect_rows > 0:
            rows = srcCur.fetchall()
            for row in rows:
                icnt += 1
                srcId = row[0]
                logging.debug("process srcId = {0}".format(srcId))
                mobile = row[1]
                mm_status = 'RDY'
                callId = row[2]
                cid = row[3]
                prjid = row[4]
                content = row[5]
                user_id = row[6]
                dateline = row[7]
                detect_result = row[8]
                chkmemo = row[9]
                sql_pre_cc_call = ''' select b.duration,b.recfileid from pre_cc_call b where b.id = {0} '''.format(callId)
                effect_rows_pre_cc_call = srcCur.execute(sql_pre_cc_call)
                if effect_rows_pre_cc_call > 0:
                    rows_pre_cc_call = srcCur.fetchone()
                    duration = rows_pre_cc_call[0]
                    recfileid = rows_pre_cc_call[1]
                    sql_pre_cc_recfile = ''' select c.dirid,c.filename from pre_cc_recfile c where c.id = {0} '''.format(recfileid)
                    effect_rows_pre_cc_recfile = srcCur.execute(sql_pre_cc_recfile)
                    if effect_rows_pre_cc_recfile > 0:
                        rows_pre_cc_recfile = srcCur.fetchone()
                        dirid = rows_pre_cc_recfile[0]
                        filename = rows_pre_cc_recfile[1]
                        
                    else:
                        logging.error("未找到 pre_cc_recfile 记录 sql = {0}".format(sql_pre_cc_recfile))
                        # 20190325 关于手工补单bug优化
                        sql_mm = ''' select id from (select id from mm_record_quality where mobile = {0} and status != 'RDY' order by create_time desc) a group by a.id '''.format(mobile)
                        effect_rows_mm = destCur.execute(sql_mm)
                        if effect_rows_mm > 0:
                            rows_mm = destCur.fetchone()
                            mm_id = rows_mm[0]
                            sql_mm_recover = ''' insert into  mm_record_quality (mobile,file_name,text_file,status,score,src_id,duration,cid,prjid,company_name,project_name,content,operator_id,deal,uid,worker_no,worker_name,detect_result,call_time)
                            select mobile,file_name,text_file,status,score,{},duration,cid,prjid,company_name,project_name,'{}',operator_id,deal,uid,worker_no,worker_name,detect_result,call_time from mm_record_quality 
                             where id = {}'''.format(srcId, content, mm_id)
                            logging.debug("sql_mm_recover = {0}".format(sql_mm_recover))
                            effect_rows_mm_recover = destCur.execute(sql_mm_recover)
                            if effect_rows_mm_recover != 1:
                                logging.error("补充手工单失败 sql_mm_recover = {0}".format(sql_mm_recover))
                        else:
                            logging.error("未找到 mm_record_quality 记录 sql_mm = {0}".format(sql_mm))
                        continue
                else:
                    logging.error("未找到 pre_cc_call 记录 sql = {0}".format(sql_pre_cc_call))
                    continue
                sql_pre_cc_corp = ''' select d.memo from pre_cc_corp d where d.id = {0} '''.format(cid)
                effect_rows_pre_cc_corp = srcCur.execute(sql_pre_cc_corp)
                if effect_rows_pre_cc_corp > 0:
                    rows_pre_cc_corp = srcCur.fetchone()
                    company_name = rows_pre_cc_corp[0]
                else:
                    logging.error("未找到 pre_cc_corp 记录 sql = {0}".format(sql_pre_cc_corp))
                    continue
                sql_pre_cc_project = ''' select e.memo from pre_cc_project e where e.id = {0} '''.format(prjid)
                effect_rows_pre_cc_project = srcCur.execute(sql_pre_cc_project)
                if effect_rows_pre_cc_project > 0:
                    rows_pre_cc_project = srcCur.fetchone()
                    project_name = rows_pre_cc_project[0]
                else:
                    logging.error("未找到 pre_cc_project 记录 sql = {0}".format(sql_pre_cc_project))
                    continue
                sql_pre_cc_user = ''' select f.name workerno,f.memo workername from pre_cc_user f where f.id = {0} '''.format(user_id)
                effect_rows_pre_cc_user = srcCur.execute(sql_pre_cc_user)
                if effect_rows_pre_cc_user > 0:
                    rows_pre_cc_user = srcCur.fetchone()
                    worker_id = rows_pre_cc_user[0]
                    worker_name = rows_pre_cc_user[1]
                else:
                    logging.error("未找到 pre_cc_user 记录 sql = {0}".format(sql_pre_cc_user))
                    continue

                destFile = '{0}/{1}'.format(time.strftime('%Y%m%d/%H', time.localtime(dateline)), filename)
                logging.debug(''' process srcid = {0} filename = {1} '''.format(srcId,filename))
                # 上传文件
                if dirid == 1:
                    localPath = '{0}/{1}'.format(configer.wavPath1, filename)
                elif dirid == 2:
                    localPath = '{0}/{1}'.format(configer.wavPath2, filename)
                if not rec.checkExists(destFile):
                    rec.upload(destFile, localPath)
                    destSql = ''' insert into mm_record_quality(mobile,file_name,status,src_id,cid,prjid,duration,company_name,project_name,content,uid, worker_no, worker_name, detect_result, chkmemo, call_time) value ('{0}','{1}','{2}',{3},{4},{5},{6},'{7}','{8}','{9}',{10},'{11}','{12}','{13}','{14}',str_to_date('{15}','%Y%m%d%H%i%S')) '''.format(
                        mobile, destFile, mm_status, srcId, cid, prjid, duration, company_name, project_name, content, user_id, worker_id, worker_name, detect_result, chkmemo, time.strftime('%Y%m%d%H%M%S', time.localtime(dateline)))
                    logging.debug(destSql)
                    destCur.execute(destSql)
                else:
                    logging.info("file is already exists " + destFile)
            if icnt % 1000 == 0:
                srcDb.commit()
                destDb.commit()
                srcDb.ping(reconnect=True)
                destDb.ping(reconnect=True)
        srcDb.commit()
        destDb.commit()
        srcDb.close()
        destDb.close()
    except Exception as e:
        if srcDb is not None:
            srcDb.commit()
            srcDb.close()
        if destDb is not None:
            destDb.commit()
            destDb.close()
        logging.error(e)
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='配置文件', default='conf/record.conf')
    parser.add_argument('--logging', help='日志配置', default='conf/logging_upload.conf')
    parser.add_argument('--beginTime', help=r'开始时间，格式为%%Y%%m%%d%%H', default='None')
    parser.add_argument('--endTime', help=r'结束时间，格式为%%Y%%m%%d%%H', default='None')
    args = parser.parse_args()

    if args.logging:
        os.makedirs('log', exist_ok=True)
        logging.config.fileConfig(args.logging)
    rec = Record(args.config)

    if args.beginTime != 'None':
        beginTime = int(time.mktime((time.strptime(args.beginTime, '%Y%m%d%H'))))
        if args.endTime != 'None':
            endTime = int(time.mktime((time.strptime(args.endTime, '%Y%m%d%H'))))
        else:
            endTime = beginTime + 3600
        process(rec.configer, beginTime, endTime)
    else:
        while 1:
            beginTime = int(time.time() / 3600) * 3600 - 3600
            endTime = beginTime + 3600
            t = threading.Thread(target=process, args=(rec.configer, beginTime, endTime,))
            t.start()
            timeNow = datetime.datetime.today()
            sleepTime = 3600 - timeNow.minute * 60 - timeNow.second + 1
            logging.info("进入休眠{0}s".format(sleepTime))
            time.sleep(sleepTime)
