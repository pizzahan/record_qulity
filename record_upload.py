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

        srcSql = ''' select a.dphone,c.dirid,c.filename,a.id,a.cid,a.prjid,b.duration,d.memo,e.memo,a.content,a.uid,f.name workerno,f.memo workername,a.dateline from pre_cc_biztrack a,pre_cc_call b,pre_cc_recfile c,pre_cc_corp d,pre_cc_project e, pre_cc_user f
        where a.result=2 and a.callid = b.id and b.recfileid = c.id and a.cid = d.id and a.prjid = e.id and a.uid = f.id and a.dateline BETWEEN {0} and {1}'''.format(
            beginTime, endTime)
        effect_rows = srcCur.execute(srcSql)
        if effect_rows > 0:
            rows = srcCur.fetchall()
            for row in rows:
                mobile = row[0]
                recDir = row[1]
                fileName = row[2]
                srcId = row[3]
                cid = row[4]
                prjid = row[5]
                duration = row[6]
                company_name = row[7]
                project_name = row[8]
                content = row[9]
                user_id = row[10]
                worker_id = row[11]
                worker_name = row[12]
                dateline = row[13]
                destFile = '{0}/{1}'.format(time.strftime('%Y%m%d/%H', time.localtime(dateline)), fileName)
                logging.debug(''' process srcid = {0} filename = {1} '''.format(srcId,fileName))
                # 上传文件
                if recDir == 1:
                    localPath = '{0}/{1}'.format(configer.wavPath1, fileName)
                elif recDir == 2:
                    localPath = '{0}/{1}'.format(configer.wavPath2, fileName)
                if not rec.checkExists(destFile):
                    rec.upload(destFile, localPath)
                    destSql = ''' insert into mm_record_quality(mobile,file_name,status,src_id,cid,prjid,duration,company_name,project_name,content,uid, worker_no, worker_name) value ('{0}','{1}','RDY',{2},{3},{4},{5},'{6}','{7}','{8}',{9},'{10}','{11}') '''.format(
                        mobile, destFile, srcId, cid, prjid, duration, company_name, project_name, content, user_id, worker_id, worker_name)
                    logging.debug(destSql)
                    destCur.execute(destSql)
                else:
                    logging.info("file is already exists " + destFile)
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
