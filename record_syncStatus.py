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

        destSql = ''' select src_id from mm_record_quality where date_format(create_time,'%Y%m%d%H') BETWEEN {0} and {1}'''.format(
            tmp_beginTime, tmp_endTime)
        effect_rows = destCur.execute(destSql)
        if effect_rows > 0:
            dest_rows = destCur.fetchall()
            for dest_row in dest_rows:
                srcId = dest_row[0]
                srcSql = ''' select a.status from pre_cc_biztrack a where id = {0} '''.format(srcId)
                src_effects = srcCur.execute(srcSql)
                if src_effects == 1:
                    detect_status = srcCur.fetchone()[0]
                    destUpdateSql = ''' update mm_record_quality set detect_result = {0} where src_id={1} '''.format(
                        detect_status, srcId)
                destCur.execute(destUpdateSql)
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
    parser.add_argument('--beginTime', help='开始时间，格式为%Y%m%d%H', default='None')
    parser.add_argument('--endTime', help='结束时间，格式为%Y%m%d%H', default='None')
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
            t = threading.Thread(target=process,
                                 args=(configer, beginTime, endTime,))
            t.start()
            logging.info("进入休眠3600s")
            time.sleep(3600)
