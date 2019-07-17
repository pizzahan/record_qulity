# -*- coding: utf-8 -*-
import argparse
import logging.config
import os
import datetime
import time
import traceback

import record_config
import threading


def process(in_configer, in_beginTime, in_endTime):
    tmp_beginTime = in_beginTime.strftime('%Y%m%d%H')
    tmp_endTime = in_endTime.strftime('%Y%m%d%H')
    logging.info("process file between {0} and {1} ".format(tmp_beginTime, tmp_endTime))
    srcDb, err = in_configer.get_src_db()
    if err is not None:
        logging.error(err)
        return
    destDb, err = in_configer.get_dest_db()
    if err is not None:
        logging.error(err)
        return
    try:
        str_srcId = ''
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

        destSql = ''' select src_id,id from mm_record_quality where date_format(create_time,'%Y%m%d%H') > '{0}' and date_format(create_time,'%Y%m%d%H') <= '{1}' '''.format(
            tmp_beginTime, tmp_endTime)
        effect_rows = destCur.execute(destSql)
        if effect_rows <= 0:
            logging.info("{} 至 {} 时间段没有记录需要处理".format(tmp_beginTime,tmp_endTime))
            srcDb.commit()
            destDb.commit()
            srcDb.close()
            destDb.close()
            return
        dest_rows = destCur.fetchall()
        for dest_row in dest_rows:
            srcId = str(dest_row[0])
            if len(str_srcId) == 0:
                str_srcId = srcId
            else:
                str_srcId = '{},{}'.format(str_srcId,srcId)
        dest_dict = dict(dest_rows)
        srcSql = ''' select a.id, a.status, a.content, a.chkmemo from pre_cc_biztrack a where id in ({0}) '''.format(str_srcId)
        src_effects = srcCur.execute(srcSql)
        if src_effects <= 0:
            logging.error("t3cc 数据库未找到对应记录 id in ({})".format(str_srcId))
            srcDb.commit()
            destDb.commit()
            srcDb.close()
            destDb.close()
            return
        t3cc_rows = srcCur.fetchall()
        for row in t3cc_rows:
            srcId = row[0]
            detect_status = row[1]
            content = row[2]
            chkmemo = row[3]
            destUpdateSql = ''' update mm_record_quality set detect_result = {},content = '{}',chkmemo = '{}' where src_id={} and id = {} '''.format(
                detect_status, content, chkmemo, srcId, dest_dict.get(srcId))
            destCur.execute(destUpdateSql)

            destUpdateSql1 = ''' update pt_thread set detect_result = {},content = '{}' where record_id={} '''.format(
                detect_status, content, dest_dict.get(srcId))
            destCur.execute(destUpdateSql1)
            logging.debug("process id = {}".format(dest_dict.get(srcId)))
        logging.info("total process {} ".format(effect_rows))
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
    parser.add_argument('--logging', help='日志配置', default='conf/logging_syncStatus.conf')
    parser.add_argument('--beginTime', help=r'开始时间，格式为%%Y%%m%%d%%H', default='None')
    parser.add_argument('--endTime', help=r'结束时间，格式为%%Y%%m%%d%%H', default='None')
    args = parser.parse_args()

    if args.logging:
        os.makedirs('log', exist_ok=True)
        logging.config.fileConfig(args.logging)

    configer = record_config.AsrConfig(args.config)

    if args.beginTime != 'None':
        beginTime = datetime.datetime.strptime(args.beginTime, '%Y%m%d%H')
        if args.endTime != 'None':
            endTime = datetime.datetime.strptime(args.endTime, '%Y%m%d%H')
        else:
            endTime = beginTime + datetime.timedelta(hours=1)
        process(configer, beginTime, endTime)
    else:
        while 1:
            beginTime = datetime.datetime.today() - datetime.timedelta(hours=1)
            endTime = beginTime + datetime.timedelta(hours=1)
            process(configer, beginTime, endTime)
            timeNow = datetime.datetime.today()
            sleepTime = 3600 - timeNow.minute * 60 - timeNow.second + 1
            logging.info("进入休眠{0}s".format(sleepTime))
            time.sleep(sleepTime)
