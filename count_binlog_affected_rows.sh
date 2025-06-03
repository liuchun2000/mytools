#!/bin/bash

# 检查是否有参数传递
if [ $# -eq 0 ]; then
    echo "Usage: $0 <binlog_file1> [binlog_file2 ...]"
    exit 1
fi

# 遍历所有传入的binlog文件
for BINLOG_FILE in "$@"; do
    if [ ! -f "$BINLOG_FILE" ]; then
        echo "Error: Binlog file '$BINLOG_FILE' not found. Skipping."
        continue
    fi

    # 从binlog文件名中提取序号部分
    BINLOG_SEQ=$(echo "$BINLOG_FILE" | grep -oE '\.[0-9]+' | sed 's/^\.//')

    if [ -z "$BINLOG_SEQ" ]; then
        echo "Warning: Could not extract sequence number from '$BINLOG_FILE'. Using default output name."
        OUTPUT_FILE="trx_affected_rows.csv" # 如果提取失败，使用默认名称
    else
        OUTPUT_FILE="${BINLOG_SEQ}_trx_affected_rows.csv"
    fi

    echo "Processing binlog file: $BINLOG_FILE -> Output to $OUTPUT_FILE"
    echo "---------------------------------------------------" > "$OUTPUT_FILE" # 使用 > 覆盖，每个binlog文件对应一个输出文件

    # AWK脚本开始
    mysqlbinlog --no-defaults --base64-output=DECODE-ROWS --verbose "$BINLOG_FILE" | awk '
    BEGIN {
        in_transaction = 0;
        affected_rows = 0;
        trx_start_pos = "";
        trx_unix_timestamp = ""; # 存储 Unix timestamp
        trx_start_time_formatted = ""; # 存储格式化后的时间
        current_event_pos = ""; # 存储事件的开始位置
        update_line_count = 0;
        counting_rows = "";
        optype = "";
    }

    # 捕获 # at <pos>
    /^# at/ {
        current_event_pos = $3;
    }

    # 捕获 SET TIMESTAMP=... 行
    /^SET TIMESTAMP=/ {
        # 提取数字时间戳部分
        trx_unix_timestamp = substr($0, 15, 10);
    }


    /^BEGIN$/ {
        in_transaction = 1;
        affected_rows = 0;
        trx_start_pos = current_event_pos; # 事务开始位置
        # trx_start_time_formatted 在 SET TIMESTAMP= 捕获时就已经设置了
        counting_rows = "";
        update_line_count = 0;
        next;
    }

    /Write_rows:/ { # 对应 INSERT
        if (in_transaction) {
            counting_rows = "INSERT";
        }
        next;
    }
    /### INSERT INTO/ {
        if (in_transaction && counting_rows == "INSERT") {
            affected_rows++;
            optype = "INSERT";
        }
        next;
    }

    /Update_rows:/ { # 对应 UPDATE
        if (in_transaction) {
            counting_rows = "UPDATE";
            update_line_count = 0; # 重置计数器
        }
        next;
    }
    /### UPDATE/ {
        if (in_transaction && counting_rows == "UPDATE") {
            update_line_count++;
            affected_rows++;
            optype = "UPDATE";
        }
        next;
    }

    /Delete_rows:/ { # 对应 DELETE
        if (in_transaction) {
            counting_rows = "DELETE";
        }
        next;
    }
    /### DELETE FROM/ {
        if (in_transaction && counting_rows == "DELETE") {
            affected_rows++;
            optype = "DELETE";
        }
        next;
    }

    /^(COMMIT|ROLLBACK)/ {
        if (in_transaction) {
            # 只在 affected_rows 大于 0 时才输出
            if (affected_rows > 0) {
                # 如果时间戳没有捕获到，显示 NA
                if (trx_unix_timestamp == "") {
                    print "StartPos, " trx_start_pos ", Time, NA, Rows, " affected_rows ", op, " optype;
                } else {
                    print "StartPos, " trx_start_pos ", Time, " trx_unix_timestamp ", Rows, " affected_rows ", op, " optype;
                }
            }
            in_transaction = 0;
            affected_rows = 0;
            counting_rows = "";
            update_line_count = 0;
            # 事务结束后，重置时间戳变量，以便下一个事务能捕获到新的时间戳
            trx_unix_timestamp = "";
            trx_start_time_formatted = "";
            optype = "";
        }
        next;
    }
    ' > "$OUTPUT_FILE" # AWK脚本结束，重定向到文件 (注意：这里是AWK的结束单引号，然后才是bash的重定向)

    echo "Finished processing $BINLOG_FILE."

done

echo "All specified binlog files processed."