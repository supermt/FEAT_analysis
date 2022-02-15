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

USEFUL_LATENCIES = ["Average", "P99", "P99.99"]


def aggreate_latency_type(read_latency_dict):
    results = {x: 0 for x in USEFUL_LATENCIES}
    for key in USEFUL_LATENCIES:
        results[key] = float(read_latency_dict[key])
    return results


def extract_data_from_dir(log_dir_prefix, output_file_name, tuner="FEAT"):
    latencies_distribution = []
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, True)
        stat_csv_data = pd.read_csv(stat_csv)
        std_result = StdoutReader(stdout_file)
        value_size = log_dir.split(os.sep)[-4]
        data_row_head = [std_result.device, int(std_result.cpu_count.replace("CPU", " ")), std_result.batch_size,
                         value_size]

        latency_count = aggreate_latency_type(std_result.fillrandom_hist)

        data_row = [x for x in data_row_head]
        for key in latency_count:
            # data_row.append(key)
            data_row.append(latency_count[key])
        latencies_distribution.append(data_row)
    #
    latency_pd = pd.DataFrame(latencies_distribution,
                              columns=["device", "thread number", "batch size", "value size", "AVG", "P99", "P99.99",
                                       ])
    latency_pd = latency_pd.sort_values(
        by=["batch size", "device", "thread number", ],
        ignore_index=False)
    latency_pd = latency_pd.replace({
        "NVMeSSD": "NVMe SSD", "SATAHDD": "SATA HDD", "SATASSD": "SATA SSD"
    })
    #
    latency_pd["Tuner"] = tuner
    latency_pd.to_csv(output_file_name, index=False, sep=" ")


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False

    extract_data_from_dir("../LOG_DIR/PM_results_FEAT_value_size/",
                          "csv_results/FEAT_hyper_para/FEAT_value_size.csv", "FEAT")

    extract_data_from_dir("../LOG_DIR/PM_results_origin_value_size/",
                          "csv_results/FEAT_hyper_para/origin_value_size.csv", "Default")
