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


def extract_data_from_dir(log_dir_prefix, output_file_name, tuner="FEAT"):
    tune_and_flush_distribution = []
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, True, multi_tasks=True)
        std_result = StdoutReader(stdout_file)

        flush_gap = log_dir.split(os.sep)[-4]
        value_size = log_dir.split(os.sep)[-6].replace("bytes", "")

        data_row_head = [std_result.device, flush_gap, value_size]
        throughput = float(std_result.benchmark_results["fillrandom"][2].split(" ")[0])

        for report_file in report_csv:
            if "report.csv_0" in report_file:
                report_pd = pd.read_csv(report_file)
                break
        tune_step = 0
        for i in range(report_pd.shape[0]):
            if i == 0:
                continue
            if report_pd["batch_size"][i] != report_pd["batch_size"][i - 1] \
                    or \
                    report_pd["thread_num"][i] != report_pd["thread_num"][i - 1]:
                tune_step += 1

        tune_frequncy = round(tune_step / report_pd.shape[0], 2)

        log_info = LogRecorder(LOG_file)
        flush_speed = []  # MiB/s
        for flush_job in log_info.flush_df.iloc:
            flush_speed.append(flush_job["flush_size"] / (flush_job["end_time"] - flush_job["start_time"]))

        flush_speed_pd = pd.DataFrame(flush_speed, columns=["speed"])
        avg_speed = flush_speed_pd["speed"].mean()
        speed_stdev = flush_speed_pd["speed"].std()
        avg_speed = round(avg_speed, 2)
        speed_stdev = round(speed_stdev, 2)

        data_row = [x for x in data_row_head]

        data_row.append(tune_frequncy)
        data_row.append(avg_speed)
        data_row.append(speed_stdev)
        data_row.append(throughput)

        throughput = float(std_result.benchmark_results["fillrandom"][2].split(" ")[0])

        tune_and_flush_distribution.append(data_row)
    #
    stat_df = pd.DataFrame(tune_and_flush_distribution,
                           columns=["device", "flush gap", "value size",
                                    "tune frequency", "flush speed", "Stdev", "throughput"]).sort_values(
        by=["device", "flush gap", "value size"],
        ignore_index=False).replace({
        "NVMeSSD": "NVMe SSD", "SATAHDD": "SATA HDD", "SATASSD": "SATA SSD"
    })
    #
    stat_df["Tuner"] = tuner
    stat_df.to_csv(output_file_name, index=False, sep=" ")


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False

    extract_data_from_dir("../LOG_DIR/PM_results_fine_grain_FEA_gap/",
                          "csv_results/FEAT_hyper_para/FEA_gap.csv", "FEAT")
