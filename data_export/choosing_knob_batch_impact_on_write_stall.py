# here is an example from online-ml/river
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


def load_log_and_qps(log_file, ground_truth_csv):
    # load the data
    return LogRecorder(log_file, ground_truth_csv)


STALL_REASON = ["memtable", "level0", "pending_compaction_bytes"]
short_name_for_stall_reason = {"level0": "L0O", "pending_compaction_bytes": "RDO", "memtable": "MMO"}


def aggreate_stall_type(stall_dict):
    results = {x: 0 for x in STALL_REASON}
    for key in stall_dict:
        for stall_reason in STALL_REASON:
            if stall_reason in key:
                results[stall_reason] += int(stall_dict[key])
    return results


if __name__ == '__main__':

    log_dir_prefix = "../LOG_DIR/PM_results_1hour_batch_size/100/"
    stall_type_distribution = []
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, True)
        stat_csv_data = pd.read_csv(stat_csv)
        std_result = StdoutReader(stdout_file)
        data_row_head = [std_result.device.replace("SATA", "SATA ").replace("NVMe", "NVMe "),
                         std_result.cpu_count.replace("CPU", ""), std_result.batch_size]
        stall_number = aggreate_stall_type(std_result.stall_reasons)

        for key in stall_number:
            data_row = [x for x in data_row_head]
            data_row.append(short_name_for_stall_reason[key])
            data_row.append(stall_number[key])
            stall_type_distribution.append(data_row)

    COLUMNS = ["device", "thread number", "batch size", "stall type",
               "stall amount"]
    ORIGIN_COLUMNS = ["device", "thread number", "stall type"]
    stall_type_distribution_pd = pd.DataFrame(stall_type_distribution,
                                              columns=COLUMNS)

    stall_type_distribution_pd = stall_type_distribution_pd.sort_values(
        by=["batch size", "device", "thread number", "stall type"],
        ignore_index=False)

    print(stall_type_distribution_pd["batch size"].unique())

    stall_type_distribution_pd.to_csv("csv_results/stall_count_sorted_by_batch_size.csv", index=False, sep=" ")
