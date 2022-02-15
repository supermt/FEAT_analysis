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
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, True)
        std_result = StdoutReader(stdout_file)

        flush_gap = log_dir.split(os.sep)[-4]
        cpu_count = std_result.cpu_count.replace("CPU", "")

        data_row_head = [std_result.device, flush_gap, cpu_count]
        throughput = float(std_result.benchmark_results["fillrandom"][2].split(" ")[0])

        report_pd = pd.read_csv(report_csv)

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

        data_row.append(avg_speed)
        data_row.append(len(log_info.flush_df))
        data_row.append(len(log_info.compaction_df))

        throughput = float(std_result.benchmark_results["fillrandom"][2].split(" ")[0])

        tune_and_flush_distribution.append(data_row)
        print(data_row)
    # #
    stat_df = pd.DataFrame(tune_and_flush_distribution,
                           columns=["device", "flush gap", "CPU count", "flush speed", "flush jobs",
                                    "compaction jobs"]).sort_values(
        by=["device", "flush gap", "CPU count"],
        ignore_index=False).replace({
        "NVMeSSD": "NVMe SSD", "SATAHDD": "SATA HDD", "SATASSD": "SATA SSD"
    })
    #
    stat_df["Tuner"] = tuner
    stat_df.to_csv(output_file_name, index=False, sep=" ")


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False

    extract_data_from_dir("../LOG_DIR/PM_results_traversal_1hour/",
                          "csv_results/NVMeSSD_flush_speed.csv", "Default")
