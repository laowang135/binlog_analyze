# binlog_analyze
# binlog_analyze_1020.py 介绍
脚本使用方式：binlog_analyze_1020.py binlog_file_path
脚本会根据传参的binlog文件解析出(--base64-output=decode-rows -vvv)一个对应的binlogfilename.log文件,工具会根据该文件，输出事务GTID值、开始时间、提交时间、事务影响行数、SQL语句类型以及SQL语句信息。

# binlog_analyze_1021.py 介绍
脚本使用方式：binlog_analyze_1020.py binlog_file_path/binlog_file.txt
脚本会根据传参的文件判断是否需要进行解析，若后缀名为.txt结尾的文件则直接进行数据处理,否则使用mysqlbinlog工具解析(--base64-output=decode-rows -v)出一个对应的binlogfilename.txt文件,工具会根据该文件，输出事务GTID值、开始时间、提交时间、事务影响行数、SQL语句类型。

