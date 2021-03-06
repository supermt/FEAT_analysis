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

FONT_SIZE = 28

STALL_REASON = ["level0", "pending_compaction_bytes", "memtable"]


def aggreate_stall_type(stall_dict):
    results = {x: 0 for x in STALL_REASON}
    for key in stall_dict:
        for stall_reason in STALL_REASON:
            if stall_reason in key:
                results[stall_reason] += int(stall_dict[key])
    return results


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False

    log_dir_prefix = "../LOG_DIR/PM_results_traversal_1hour/"

    # origin_painting_df = pd.DataFrame()
    export_csv_list = []

    stall_type_distribution = []
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, True)
        stat_csv_data = pd.read_csv(stat_csv)
        std_result = StdoutReader(stdout_file)
        data_row_head = [std_result.device, int(std_result.cpu_count.replace("CPU", ""))]

        export_row = []
        export_row.extend(data_row_head)
        stall_number = aggreate_stall_type(std_result.stall_reasons)

        for key in stall_number:
            export_row.append(stall_number[key])

        export_csv_list.append(export_row)

    print(export_csv_list)

    origin_export_df = pd.DataFrame(export_csv_list, columns=["device", "thread number", "L0 Stall",
                                                         "PendingBytes Stall", "MemtableStall"])
    print(origin_export_df)

    stall_type_distribution_pd = origin_export_df.sort_values(by=["device", "thread number"],
                                                              ignore_index=True)

    stall_type_distribution_pd.to_csv("csv_results/stall_changing.csv")
