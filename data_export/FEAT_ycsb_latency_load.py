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

USEFUL_LATENCIES = ["P99", "P99.99"]


def aggreate_latency_type(keys, values):
    results = {x: 0 for x in USEFUL_LATENCIES}
    i = 0
    for key in keys:
        if key in USEFUL_LATENCIES:
            results[key] = float(values[i])
        i += 1

    return results


def append_latency_list(input_list, workload, data_row_head):
    latency_stats = []
    i = 0
    for hist_dict in input_list:
        op_type = hist_dict[0]
        if i == 0:
            stage = "load"
        else:
            stage = "run"
            pass

        latency_count = aggreate_latency_type(hist_dict[1], hist_dict[2])
        print(stage, workload)
        if stage != "load":
            pass
        else:
            if workload not in ["A", "D", "E"]:
                pass
            else:
                for key in latency_count:
                    data_row = [x for x in data_row_head]
                    data_row.append(workload)
                    data_row.append(stage)
                    data_row.append(hist_dict[0])
                    data_row.append(key)
                    data_row.append(latency_count[key])
                    if latency_count[key] == 0:
                        pass
                    else:
                        latency_stats.append(data_row)
        i += 1
    return latency_stats


def extract_data_from_dir(log_dir_prefix, output_file_name, tuner="FEAT"):
    latencies = []
    throughputs = []
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:

        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, True, multi_tasks=True)
        stat_csv_data = pd.read_csv(stat_csv)
        std_result = StdoutReader(stdout_file)
        workload = log_dir.split(os.sep)[-4]

        data_row_head = [std_result.device, ]  # int(std_result.cpu_count.replace("CPU", " ")), std_result.batch_size]

        latencies.extend(append_latency_list(std_result.hist_job_list, workload.upper(), data_row_head))

        for stage in ["load"]:
            throughput_row = []
            throughput_row.extend(data_row_head)
            throughput_row.append(workload)
            throughput_row.append(stage)
            throughput_row.append(float(std_result.benchmark_results["ycsb_" + stage][1].split(" ")[0]))
            throughputs.append(throughput_row)

    latency_pd = pd.DataFrame(latencies,
                              columns=["device", "workload", "stage", "op_type",
                                       "latency type", "load latency"])
    latency_pd = latency_pd.sort_values(
        by=["stage", "op_type", "workload", "device", "latency type"],
        ignore_index=False)
    latency_pd = latency_pd.replace({
        "NVMeSSD": "NVMe SSD", "SATAHDD": "SATA HDD", "SATASSD": "SATA SSD"
    })
    #
    latency_pd["Tuner"] = tuner
    latency_pd.to_csv(output_file_name + "_latency.csv", index=False, sep=" ")

    throughput_pd = pd.DataFrame(throughputs,
                                 columns=["device", "workload", "stage",
                                          "throughput"]).sort_values(
        by=["device"],
        ignore_index=False).replace({
        "NVMeSSD": "NVMe SSD", "SATAHDD": "SATA HDD", "SATASSD": "SATA SSD"
    })
    throughput_pd["Tuner"] = tuner

    throughput_pd.to_csv(output_file_name + "loading_throughput.csv", index=False, sep=" ")


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False

    extract_data_from_dir("../LOG_DIR/PM_results_ycsb_load_60GB/baseline/",
                          "csv_results/ycsb_report/ycsb_baseline", "Default")
    extract_data_from_dir("../LOG_DIR/PM_results_ycsb_load_60GB/FEAT/",
                          "csv_results/ycsb_report/ycsb_FEAT", "FEAT")
    # extract_data_from_dir("../LOG_DIR/PM_results_ycsb_baseline/",
    #                       "csv_results/ycsb_report/ycsb_baseline.csv", "SILK")
