#!/bin/python
# -*- coding: utf8 -*-
# DESC 分析binlog日志，生成事务耗时、更新的行数等信息。
# Python2、Python3环境均可使用
import os
import sys
import re
import time
import subprocess

# 检查参数数量
if len(sys.argv) != 2:
    print("Usage: python script_name.py binlog_filename/binlog_file.txt")
    sys.exit(1)

# binlog文件路径
binlog_name = sys.argv[1]  

# 检查binlog文件是否存在
if not os.path.isfile(binlog_name):
    print("File '%s' does not exist." % binlog_name)
    sys.exit(1)

# 判断传入的文件是否以.txt结尾，如果是，不进行binlog解析，直接使用该文件进行处理
if binlog_name.endswith('.txt'):
    output_log_name = binlog_name
else:
    binlog_filename = os.path.basename(binlog_name)  # binlog文件名称
    output_log_name = os.path.join(binlog_filename + '.txt')  # 输出日志文件名
    # 执行mysqlbinlog命令并将输出重定向到日志文件
    print("Begin decoding the binlog file")
    command = "mysqlbinlog --base64-output=decode-rows -v %s > %s" % (binlog_name, output_log_name)
    subprocess.call(command, shell=True)

print "{0: <50} {1: <20} {2: <20} {3: <10} {4: <10}".format("trx_id", "trx_start", "trx_commit", "sql_type", "trx_rows"), "trx_sql"

with open(output_log_name, 'r') as f:
    while True:
        line = f.readline()
        if not line:
            break
        # 事务提交时间
        match = re.match(r"# immediate_commit_timestamp=.* \((.*)\..*", line)
        if match:
            commit_time = match.group(1)
            nums = 0
            gtid = None
            start_time = None
            trx_type = None
            trx_sql = []
            while True:
                line = f.readline()
                # 记录事务GTID
                if gtid is None:
                    match = re.match(r"(SET @@SESSION.GTID_NEXT=) '(.*?\d)'.*;", line)
                    if match:
                        gtid = match.group(2)
                        continue
                # 记录事务开始时间
                if start_time is None:
                    match = re.match(r"(SET TIMESTAMP)=(\d{10}).*;", line)
                    if match:
                        start_time = match.group(2)
                        start_time_obj = time.localtime(int(start_time))
                        start_time = time.strftime("%Y-%m-%d %H:%M:%S",start_time_obj)
                        continue
                # 记录事务类型
                match = re.search(r"### (INSERT|DELETE|UPDATE) (.*)", line,re.I)
                if match:
                    trx_type = 'DML'
                    sql = match.group(1,2)
                    if sql not in trx_sql:
                        trx_sql.append(sql)
                    nums = nums + 1
                    continue
                if trx_type is None:
                    match = re.search(r"(CREATE|DROP|ALTER|TRUNCATE) (.*)", line,re.I)
                    if match:
                        trx_type = 'DDL'
                        sql = match.group(1,2)
                        nums = "None"
                        end_time = "None"
                        #print(gtid, '"' + start_time + '"', '"' + commit_time + '"', trx_type, nums, sql)
                        print "{0: <50} {1: <20} {2: <20} {3: <10} {4: <10}".format(gtid, start_time, commit_time,trx_type, nums), sql

                        break
                # 事务结束
                match = re.match(r"(^COMMIT).*", line)
                if match:
                    #print(gtid, '"' + start_time + '"', '"' + commit_time + '"', trx_type, nums, trx_sql)
                    print "{0: <50} {1: <20} {2: <20} {3: <10} {4: <10}".format(gtid,start_time,commit_time,trx_type,nums),trx_sql
                    break
