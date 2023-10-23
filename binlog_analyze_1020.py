#!/bin/python
# -*- coding: utf8 -*-
# VERSION 2.0
# DATE 2023-10-20
# DESC 分析binlog日志，生成事务耗时、更新的行数等信息。

import os
import sys
import re
import time
import subprocess

if len(sys.argv) != 2:
    print("Usage: python script_name.py binlog_filename")
    sys.exit(1)
binlog_name = sys.argv[1] # binlog文件路径
if not os.path.isfile(binlog_name):
    print("File '%s' does not exist." % binlog_name)
    sys.exit(1)
binlog_filename = os.path.basename(binlog_name) # binlog文件名称
output_log_name = os.path.join(binlog_filename + '.log') # 输出日志文件名

# binlog_name = '/root/scripts/binlog.000042' # 你的binlog文件名
# output_log_name = '/root/scripts/binlog.000042.log'

# 执行mysqlbinlog命令并将输出重定向到日志文件
print("Begin decoding the binlog file")
command = "mysqlbinlog --base64-output=decode-rows -vvv %s > %s" % (binlog_name, output_log_name)
subprocess.call(command, shell=True)

if os.path.exists(output_log_name):
    pass
    print("The binary log file is decoded successfully,and the file name is",output_log_name)
else:
    print('File %s does not exist' % output_log_name)
    sys.exit(1)
# 打印输出
def print_transaction_info(gtid, start_time, commit_time, sql_list):
    print("GTID:", gtid)
    print("Start Time:", start_time)
    print("Commit Time:", commit_time)
    print("Transaction Counts:",sql_list[-1][1])
    
    for trx_type,nums,sql in sql_list:
        print("SQL Type:", trx_type.upper())
        print("SQL:",sql.lower())
    print("\n")

sql_list = []
search_type = "initial_type"
with open(output_log_name,'r') as f:
#with open('binlog.000005.txt','r') as f:
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
                match = re.search(r"^(#|###) (REPLACE|INSERT|DELETE|UPDATE) (.*)", line, re.I)
                if match:
                    if line.startswith("###"):
                        nums += 1
                        dml_sql=(trx_type,nums,merged_sql)
                        sql_list.append(dml_sql)
                        continue
                    trx_type = match.group(2)
                    sql = match.group(2,3)
                    merged_sql = ' '.join(sql)
                    continue
                if trx_type is None:
                    match = re.search(r"(CREATE|DROP|ALTER|TRUNCATE) (.*)", line,re.I)
                    if match:
                        sql = match.group(1,2)
                        merged_sql = ' '.join(sql)
                        # SQL跨行判断拼接
                        if line.endswith('\n'):
                            trx_type_type = match.group(1)
                            search_type = "continue_type"
                            continue
                    elif search_type == "continue_type":
                        if line.endswith('\n') and not '/*!*/;' in line:
                            merged_sql += line.replace("\n", " ")
                            search_type = "continue_type"
                            continue
                        nums = "None"
                        end_time = "None"
                        trx_type = trx_type_type
                        ddl_sql=(trx_type,nums,merged_sql)
                        sql_list.append(ddl_sql)
                        search_type = "initial_type"
                        print_transaction_info(gtid, start_time, commit_time, sql_list)
                        sql_list=[]
                        break
                # 事务结束
                match = re.match(r"(^COMMIT).*", line)
                if match:
                    print_transaction_info(gtid, start_time, commit_time, sql_list)
                    sql_list=[]
                    break