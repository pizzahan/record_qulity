# -*- coding: utf8 -*-
import argparse
import logging.config
import os
import signal
import time
import traceback

import jieba
import record_config


# 分词处理
def text_split(text, local_dict):
    jieba.load_userdict(local_dict)
    res_words = list(jieba.cut(text))
    return res_words


# 统计自定义词典里词汇出现的频次
def statistic_local_words(ws, word_dic):
    cnt = 0
    res_dic = {}
    for word in word_dic:
        res_dic[word] = ws.count(word)
        cnt += ws.count(word)
    return cnt, res_dic


# 加载本地词库到字典
def load_words(path):
    passive_word_dic = {}
    f = open(path, 'r', encoding='utf-8')
    for line in f.readlines():
        line = line.strip()
        passive_word_dic[line] = 0
    f.close()
    return passive_word_dic


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='配置文件', default='conf/record.conf')
    parser.add_argument('--logging', help='日志配置', default='conf/logging_grade.conf')
    args = parser.parse_args()

    if args.logging:
        os.makedirs('log', exist_ok=True)
        logging.config.fileConfig(args.logging)

    configer = record_config.AsrConfig(args.config)

    local_dic = load_words(configer.localDict)

    while 1:
        icnt = 0
        destDb, err = configer.get_dest_db()
        if err is not None:
            logging.error(err)
            exit(-1)
        try:
            sql = ''' select id,text_file from mm_record_quality where status = 'CVT' order by create_time desc limit {0} '''.format(configer.limit)

            destCur = destDb.cursor()
            effectRows = destCur.execute(sql)
            if effectRows > 0:
                logging.info("begin to process {0} records".format(effectRows))
                rows = destCur.fetchall()
                for row in rows:
                    icnt += 1
                    idIdx = row[0]
                    fileName = row[1]
                    logging.info("begin to process id = {0}".format(idIdx))
                    fr = open(fileName, 'r', encoding='utf-8')
                    content = fr.read()
                    fr.close()
                    words = text_split(content, configer.localDict)
                    score, statistic_dic = statistic_local_words(words, local_dic)
                    for k in statistic_dic:
                        if statistic_dic[k] > 0:
                            insertSql = ''' insert into mm_record_word_statistic value ({0},'{1}',{2})'''.format(idIdx, k,
                                                                                                                 statistic_dic[
                                                                                                                     k])
                            destCur.execute(insertSql)
                    updateSql = ''' update mm_record_quality set status = 'GRD',score = {0} where id = {1} '''.format(
                        score, idIdx)
                    destCur.execute(updateSql)
                    if icnt % 1000 == 0:
                        destDb.commit()
                        destDb.ping(reconnect=True)
            else:
                destDb.commit()
                destDb.close()
                logging.info("进入休眠{0}s".format(configer.interval))
                time.sleep(configer.interval)
        except Exception as e:
            if destDb is not None:
                destDb.commit()
                destDb.close()
            logging.error(e)
            logging.error(traceback.format_exc())