#!/bin/bash

# 生成1800万个块的脚本
# 调用12000-thread.sh 15次，每次传递不同的baseDir参数

# 设置目标文件系统（与12000-thread.sh保持一致）
export TARGET_FS=hdfs://xafj-sys-rpm58y98bhi.xafj.baidu.com:8020

echo "================================================"
echo "开始生成1800万个块的测试"
echo "================================================"
echo "总执行次数: 15次"
echo "每次生成: 120万个块 (300线程 × 100文件 × 40 maps)"
echo "总生成块数: 18,000,000个"
echo "================================================"

# 主循环，执行15次
for ((i=1; i<=15; i++))
do
    echo ""
    echo "================================================"
    echo "执行第 $i 次 (共15次)"
    echo "================================================"

    # 为每次执行创建唯一的目录名
    BASE_DIR_NAME="distributed_test_run_$i"

    echo "使用基础目录: $BASE_DIR_NAME"
    echo "开始时间: $(date)"

    # 调用12000-thread.sh并传递baseDir参数
    echo "执行命令: ./12000-thread.sh $BASE_DIR_NAME"
    sh ./12000-thread-no-clean.sh $BASE_DIR_NAME

    # 检查执行状态
    if [ $? -eq 0 ]; then
        echo "第 $i 次执行成功完成"
        echo "输出目录: ${TARGET_FS}/test/$BASE_DIR_NAME"
    else
        echo "警告: 第 $i 次执行可能失败"
        echo "继续执行下一次..."
    fi

    echo "结束时间: $(date)"
    echo "已生成块数: $(($i * 1200000))"

    # 可选：在每次执行之间添加延迟，避免系统过载
    if [ $i -lt 15 ]; then
        echo "等待10秒后继续下一次执行..."
        sleep 10
    fi
done

echo ""
echo "================================================"
echo "所有15次执行已完成"
echo "================================================"
echo "总生成块数: 18,000,000个"
echo "生成的测试目录:"
for ((i=1; i<=15; i++))
do
    echo "  ${TARGET_FS}/test/distributed_test_run_$i"
done
echo ""
echo "清理建议:"
echo "如需清理这些测试目录，可以执行以下命令:"
echo "for i in {1..15}; do hadoop fs -rm -r ${TARGET_FS}/test/distributed_test_run_\$i; done"
echo "================================================"

# 可选：生成执行报告
echo "生成执行报告..."
echo "执行报告 - $(date)" > execution_report.txt
echo "======================" >> execution_report.txt
echo "总执行次数: 15" >> execution_report.txt
echo "目标文件系统: $TARGET_FS" >> execution_report.txt
echo "生成时间: $(date)" >> execution_report.txt
echo "" >> execution_report.txt
echo "生成的测试目录:" >> execution_report.txt
for ((i=1; i<=15; i++))
do
    echo "  ${TARGET_FS}/test/distributed_test_run_$i" >> execution_report.txt
done
echo "" >> execution_report.txt
echo "总生成块数: 18,000,000个" >> execution_report.txt

echo "执行报告已保存到: execution_report.txt"