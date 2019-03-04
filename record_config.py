#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import oss2
import pymysql
import configparser


class AsrConfig(object):
    def __init__(self, config):
        cf = configparser.ConfigParser()
        cf.read(config)
        # 创建Bucket对象，所有Object相关的接口都可以通过Bucket对象来进行
        self.accessKeyId = cf.get('ali', 'accessKeyId')
        self.accessKeySecret = cf.get('ali', 'accessKeySecret')
        self.bucketName = cf.get('ali', 'bucketName')
        self.endpoint = cf.get('ali', 'endpoint')
        self.expires = cf.getint('ali', 'expires')
        self.interval = cf.getint('ali', 'interval')
        self.limit = cf.getint('ali', 'limit')
        self.bucket = oss2.Bucket(oss2.Auth(self.accessKeyId, self.accessKeySecret), self.endpoint,
                                  self.bucketName)

        self.wavPath1 = cf.get('data', 'wavPath1')
        self.wavPath2 = cf.get('data', 'wavPath2')
        self.textPath = cf.get('data', 'textPath')
        self.localDict = cf.get('data', 'localDict')
        # mysql config
        self.srcHost = cf.get('mysql', 'srcHost')
        self.srcPort = cf.getint('mysql', 'srcPort')
        self.srcUser = cf.get('mysql', 'srcUser')
        self.srcPassword = cf.get('mysql', 'srcPassword')
        self.srcDbName = cf.get('mysql', 'srcDbName')
        self.srcDb = None
        # mysql config
        self.destHost = cf.get('mysql', 'destHost')
        self.destPort = cf.getint('mysql', 'destPort')
        self.destUser = cf.get('mysql', 'destUser')
        self.destPassword = cf.get('mysql', 'destPassword')
        self.destDbName = cf.get('mysql', 'destDbName')
        self.destDb = None

    def get_src_db(self):
        if self.srcDb is None:
            try:
                dbconn = pymysql.connect(host=self.srcHost,
                                         user=self.srcUser,
                                         passwd=self.srcPassword,
                                         port=self.srcPort,
                                         db=self.srcDbName,
                                         charset="utf8")
                self.srcDb = dbconn
            except Exception as err:
                return None, err
        else:
            try:
                self.srcDb.ping()
            except Exception as err:
                return None, err
        return self.srcDb, None

    def get_dest_db(self):
        if self.destDb is None:
            try:
                dbconn = pymysql.connect(host=self.destHost,
                                         user=self.destUser,
                                         passwd=self.destPassword,
                                         port=self.destPort,
                                         db=self.destDbName,
                                         charset="utf8")
                self.destDb = dbconn
            except Exception as err:
                return None, err
        else:
            try:
                self.destDb.ping()
            except Exception as err:
                return None, err
        return self.destDb, None
