# here is an example from online-ml/river
import os
from sys import prefix
from turtle import color

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
from plotly.graph_objs import Layout

from feature_selection import vectorize_by_disk_op_distribution, combine_vector_with_qps
from log_class import LogRecorder
from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_and_std_files, mkdir_p
from utils.traversal import get_log_dirs
import plotly.graph_objs as go

FONT_SIZE = 28
PM_device_mapper = {
    "SATAHDD": "sdc",
    "SATASSD": "sda",
    "NVMeSSD": "nvme0n1",
    "PM": "pmem0"
}

IOSTAT_COLUMN_NAMES = "Device             tps    MB_read/s    MB_wrtn/s    MB_read    MB_wrtn"
IOSTAT_COLUMN = IOSTAT_COLUMN_NAMES.split()


def extract_data_from_dir(log_dir_prefix, output_file_name, tuner="FEAT"):
    trade_off_metrics = []
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv, stat_csv, IO_stat = get_log_and_std_files(log_dir, True, True,
                                                                                     multi_tasks=True)
        stat_csv_data = pd.read_csv(stat_csv)
        std_result = StdoutReader(stdout_file)
        idle_rate = log_dir.split(os.sep)[-4]
        value_size = log_dir.split(os.sep)[-5].replace("bytes", "")

        data_row_head = [std_result.device, idle_rate, value_size]
        report_csv_data = pd.read_csv(report_csv[0])
        used_report_data = report_csv_data[["secs_elapsed", "thread_num"]]

        used_stat_data = stat_csv_data[["secs", "cpu_percent", "num_threads"]]

        cpu_utils = []
        for sec_row in used_report_data.iloc:
            sec = sec_row["secs_elapsed"]
            stat_row = used_stat_data[used_stat_data["secs"] == sec]
            if not stat_row.empty:
                cpu_utils.append([sec, float(stat_row["cpu_percent"]), sec_row["thread_num"]])

        cpu_utils_pd = pd.DataFrame(cpu_utils, columns=["sec", "cpu_util", "threads"])
        average_cpu = round((cpu_utils_pd["cpu_util"] / cpu_utils_pd["threads"]).mean(), 2)
        max_cpu = cpu_utils_pd["cpu_util"].max()

        iostat_lines = open(IO_stat, "r", encoding="utf-8").readlines()
        IOSTAT = []
        for line in iostat_lines:
            if PM_device_mapper[std_result.device] in line:
                IOSTAT.append(line.split())
        IOSTAT = pd.DataFrame(IOSTAT, columns=IOSTAT_COLUMN)
        MBPS = IOSTAT["MB_wrtn/s"].astype(float)
        max_IO_bandwidth = MBPS.max()
        average_IO_bandwidth = MBPS.mean()

        throughput = float(std_result.benchmark_results["fillrandom"][2].split(" ")[0])

        data_row = [x for x in data_row_head]
        data_row.append(max_cpu)
        data_row.append(average_cpu)
        data_row.append(max_IO_bandwidth)
        data_row.append(average_IO_bandwidth)

        data_row.append(throughput)
        trade_off_metrics.append(data_row)

    stat_pd = pd.DataFrame(trade_off_metrics,
                           columns=["device", "idle ratio", "value size",
                                    "max_cpu", "avg_cpu",
                                    "max_io", "avg_io",
                                    "throughput"]).sort_values(
        by=["device", "idle ratio", "value size"],
        ignore_index=False).replace({
        "NVMeSSD": "NVMe SSD", "SATAHDD": "SATA HDD", "SATASSD": "SATA SSD"
    })
    #
    stat_pd.to_csv(output_file_name, index=False, sep=" ")


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False
    # idle ratio is related to the bandwidth and cpu utils
    extract_data_from_dir("../LOG_DIR/PM_results_idle_ratio_fine_grain/",
                          "csv_results/FEAT_hyper_para/idle_rate.csv", "FEAT")
