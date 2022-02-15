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


def prettify_the_fig(fig):
    fontsize = FONT_SIZE
    fig.update_layout(legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.1,
        xanchor="center",
        x=0.5,
        font=dict(size=fontsize)))
    layout = Layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )

    fig.update_layout(layout)
    # share the titles
    fig.update_yaxes(title='')
    # remove the "="
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

    fig.update_layout(font=dict(size=fontsize), autosize=False,
                      width=1200,
                      height=900
                      )


def set_title(fig):
    fig.update_layout(yaxis7=dict(title="Stall Amount (times)", titlefont=dict(size=FONT_SIZE + 4)))


def load_log_and_qps(log_file, ground_truth_csv):
    # load the data
    return LogRecorder(log_file, ground_truth_csv)


def data_cleaning_by_max_MBPS(bucket_df, MAX_READ=2000, MAX_WRITE=1500):
    read = bucket_df["read"]
    bad_read = read >= MAX_READ
    read[bad_read] = MAX_READ
    write = bucket_df["write"]
    bad_write = write >= MAX_WRITE
    write[bad_write] = MAX_WRITE
    return bucket_df


STALL_REASON = ["level0", "pending_compaction_bytes", "memtable"]


def aggreate_stall_type(stall_dict):
    results = {x: 0 for x in STALL_REASON}
    for key in stall_dict:
        for stall_reason in STALL_REASON:
            if stall_reason in key:
                results[stall_reason] += int(stall_dict[key])
    return results


def load_log_and_qps(log_file, ground_truth_csv):
    # load the data
    return LogRecorder(log_file, ground_truth_csv)


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False

    log_dir_prefix = "LOG_DIR/PM_server_results_hist_read_after_write/"
    stall_type_distribution = []
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, True)
        stat_csv_data = pd.read_csv(stat_csv)
        std_result = StdoutReader(stdout_file)
        data_row_head = [std_result.device, int(std_result.cpu_count.replace("CPU", "")), std_result.batch_size]

        compaction_list = load_log_and_qps(LOG_file, report_csv).compaction_df
        compaction_list["drop_rate"] = compaction_list["num_output_records"] / compaction_list["num_input_records"]
        l0_compactions = compaction_list[compaction_list["compaction_reason"] == "LevelL0FilesNum"]
        other_compactions = compaction_list[compaction_list["compaction_reason"] == "LevelMaxLevelSize"]
        print(l0_compactions["drop_rate"].describe())
        print(other_compactions["drop_rate"].describe())
