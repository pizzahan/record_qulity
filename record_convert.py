# -*- coding: utf8 -*-
import argparse
import json
import logging.config
import os
import signal
import threading
import time
import traceback

import jieba
from aliyunsdkcore.acs_exception.exceptions import ClientException
from aliyunsdkcore.acs_exception.exceptions import ServerException
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest
import record_config


def file_trans(acc_list, link):
    result = ""
    for accDic in acc_list:
        akId = accDic.get("accessKeyId")
        akSecret = accDic.get("accessKeySecret")
        appKey = accDic.get("appKey")
        status = accDic.get("status")
        if not status:
            result = "USER_BIZDURATION_QUOTA_EXCEED"
            continue
        else:
            result = ""
            REGION_ID = "cn-shanghai"
            PRODUCT = "nls-filetrans"
            DOMAIN = "filetrans.cn-shanghai.aliyuncs.com"
            API_VERSION = "2018-08-17"
            POST_REQUEST_ACTION = "SubmitTask"
            GET_REQUEST_ACTION = "GetTaskResult"
            KEY_APP_KEY = "app_key"
            KEY_FILE_LINK = "file_link"
            KEY_TASK = "Task"
            KEY_TASK_ID = "TaskId"
            KEY_STATUS_TEXT = "StatusText"
            # 创建AcsClient实例
            client = AcsClient(akId, akSecret, REGION_ID)
            # 创建提交录音文件识别请求，并设置请求参数
            postRequest = CommonRequest()
            postRequest.set_domain(DOMAIN)
            postRequest.set_version(API_VERSION)
            postRequest.set_product(PRODUCT)
            postRequest.set_action_name(POST_REQUEST_ACTION)
            postRequest.set_method('POST')
            task = {KEY_APP_KEY: appKey, KEY_FILE_LINK: link}
            task = json.dumps(task)
            postRequest.add_body_params(KEY_TASK, task)
            taskId = ""
            try:
                # 提交录音文件识别请求，处理服务端返回的响应
                postResponse = client.do_action_with_exception(postRequest)
                postResponse = json.loads(postResponse)
                # 获取录音文件识别请求任务的ID，以供识别结果查询使用
                statusText = postResponse[KEY_STATUS_TEXT]
                if statusText == "SUCCESS":
                    logging.info("录音文件识别请求成功响应！")
                    taskId = postResponse[KEY_TASK_ID]
                elif statusText == "USER_BIZDURATION_QUOTA_EXCEED":
                    logging.info(akId + " 时长用尽，切换其他用户尝试！" + statusText)
                    result = statusText
                    accDic['status'] = False
                    continue
                else:
                    logging.info("录音文件识别请求失败！" + statusText)
                    return ""
            except ServerException as e:
                logging.info(e)
            except ClientException as e:
                logging.info(e)
            # 创建识别结果查询请求，设置查询参数为任务ID
            getRequest = CommonRequest()
            getRequest.set_domain(DOMAIN)
            getRequest.set_version(API_VERSION)
            getRequest.set_product(PRODUCT)
            getRequest.set_action_name(GET_REQUEST_ACTION)
            getRequest.set_method('GET')
            getRequest.add_query_param(KEY_TASK_ID, taskId)
            # 提交录音文件识别结果查询请求
            # 以轮询的方式进行识别结果的查询，直到服务端返回的状态描述符为"SUCCESS"、"SUCCESS_WITH_NO_VALID_FRAGMENT"，
            # 或者为错误描述，则结束轮询。
            while True:
                try:
                    getResponse = client.do_action_with_exception(getRequest)
                    getResponse = json.loads(getResponse)
                    statusText = getResponse[KEY_STATUS_TEXT]
                    if statusText == "RUNNING" or statusText == "QUEUEING":
                        # 继续轮询
                        time.sleep(3)
                    else:
                        # 退出轮询
                        Result = getResponse.get("Result")
                        if Result is not None:
                            Sentences = Result.get("Sentences")
                            if len(Sentences) > 0:
                                for Sentence in Sentences:
                                    result = result + Sentence.get("Text", "")
                        break
                except ServerException as e:
                    logging.error(e)
            if statusText == "SUCCESS" or statusText == "SUCCESS_WITH_NO_VALID_FRAGMENT":
                logging.info("录音文件识别成功！")
                break
            else:
                logging.error("录音文件识别失败！" + fileLink)
    return result


# 分词处理
def text_split(text, local_dict):
    jieba.load_userdict(local_dict)
    words = list(jieba.cut(text))
    return words


# 统计自定义词典里词汇出现的频次
def statistic_local_words(words, word_dic):
    for word in word_dic:
        word_dic[word] = words.count(word)


# 加载本地词库到字典
def load_words(path):
    passive_word_dic = {}
    f = open(path, 'r', encoding='utf-8')
    for line in f.readlines():
        line = line.strip()
        passive_word_dic[line] = 0
    f.close()
    return passive_word_dic


# 保存转文字结果
def save_text(text, dest_path):
    fw = open(dest_path, 'w', encoding='utf-8', errors='ignore')
    fw.write(text)
    fw.close()


# 获取ali配置
def get_accs(cf):
    al = []
    accDb, e = cf.get_dest_db()
    if e is not None:
        logging.error(e)
        exit(-1)
    accSql = 'select ak,aks,apk from mm_ali'
    accCur = accDb.cursor()
    efrs = accCur.execute(accSql)
    if efrs > 0:
        ali_rows = accCur.fetchall()
        for ali_row in ali_rows:
            ak = ali_row[0]
            aks = ali_row[1]
            apk = ali_row[2]
            al.append({'accessKeyId': ak, 'accessKeySecret': aks, 'appKey': apk, 'status': True})
    accDb.commit()
    accDb.close()
    return al


def reset_acc(acc_list):
    while 1:
        time.sleep(3600 * 24)
        for accDic in acc_list:
            logging.info("reset account list status day by day!")
            accDic['status'] = True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='配置文件', default='conf/record.conf')
    parser.add_argument('--logging', help='日志配置', default='conf/logging_convert.conf')
    args = parser.parse_args()

    if args.logging:
        os.makedirs('log', exist_ok=True)
        logging.config.fileConfig(args.logging)
    configer = record_config.AsrConfig(args.config)
    accList = get_accs(configer)
    if len(accList) > 0:
        t = threading.Thread(target=reset_acc, args=(accList,))
        t.start()
        while True:
            destDb, err = configer.get_dest_db()
            if err is not None:
                logging.error(err)
                exit(-1)
            try:
                sql = ''' select id,file_name,substr(SUBSTRING_INDEX(SUBSTRING_INDEX(file_name,'_',2),'/',-1),1,11) from mm_record_quality where status = 'RDY'  order by create_time limit {0}'''.format(
                    configer.limit)

                destCur = destDb.cursor()
                effectRows = destCur.execute(sql)
                if effectRows > 0:
                    rows = destCur.fetchall()
                    for row in rows:
                        idIdx = row[0]
                        fileName = row[1]
                        subPath = row[2].replace('_', '/')

                        # 生成签名URL
                        fileLink = configer.bucket.sign_url('GET', row[1], configer.expires)
                        resText = file_trans(accList, fileLink)
                        if resText != "":
                            if resText == "USER_BIZDURATION_QUOTA_EXCEED":
                                sleep_seconds = int(time.time() / 3600 / 24) * 3600 * 24 + (24 - 8) * 3600 - int(
                                    time.time())
                                logging.error(
                                    "there's no free duration today,process will be start after {0} seconds.".format(
                                        sleep_seconds))
                                for acc in accList:
                                    acc['status'] = True
                                time.sleep(sleep_seconds)

                            destPath = configer.textPath + '/' + subPath + '/'
                            isExists = os.path.exists(destPath)
                            if not isExists:
                                os.makedirs(destPath)
                            destPath = destPath + fileName.split(r"/")[-1].split(r".")[0] + ".txt"
                            save_text(resText, destPath)
                            updateSql = ''' update mm_record_quality set status = 'CVT',text_file = '{0}' where id = {1} '''.format(
                                destPath, idIdx)
                            destCur.execute(updateSql)
                        else:
                            logging.error("识别失败 " + fileName)
                            updateSql = ''' update mm_record_quality set status = 'ERR' where id = {0} '''.format(
                                idIdx)
                            destCur.execute(updateSql)
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
    else:
        logging.error("未配置ali账户信息")
