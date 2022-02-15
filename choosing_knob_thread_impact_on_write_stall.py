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
                      height=1400
                      )


def set_title(fig):
    fig.update_layout(yaxis3=dict(title="Stall Amount (times)", titlefont=dict(size=FONT_SIZE + 4)))


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


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False

    log_dir_prefix = "LOG_DIR/PM_results_traversal_1hour/"

    # origin_painting_df = pd.DataFrame()
    stall_type_distribution = []
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, True)
        stat_csv_data = pd.read_csv(stat_csv)
        std_result = StdoutReader(stdout_file)
        data_row_head = [std_result.device, int(std_result.cpu_count.replace("CPU", "")), std_result.batch_size]
        stall_number = aggreate_stall_type(std_result.stall_reasons)

        export_row = []
        export_row.extend(data_row_head)

        for key in stall_number:
            data_row = [x for x in data_row_head]
            export_row.append(stall_number)
            data_row.append(key)
            data_row.append(stall_number[key])
            stall_type_distribution.append(data_row)

    stall_type_distribution_pd = pd.DataFrame(stall_type_distribution,
                                              columns=["device", "thread number", "batch size", "stall type",
                                                       "stall amount"])

    stall_type_distribution_pd = stall_type_distribution_pd.sort_values(by=["device", "thread number"],
                                                                        ignore_index=True)
    print(stall_type_distribution_pd)

    stall_type_distribution_pd.to_csv("paper_usage/knob_choosing/stall_changing.csv")

    stall_type_distribution_pd.to_csv("data_export/csv_results/stall_changing.csv")

    import plotly.express as px

    # devices = stall_type_distribution_pd["device"].unique()
    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    fig = px.bar(stall_type_distribution_pd, x="thread number", facet_row="device", y="stall amount",
                 color="stall type", category_orders={
            "device": devices,
        },
                 labels={"device": "", "stall_amount": ""})
    max_amount_each_device = {}
    devices = stall_type_distribution_pd["device"].unique()
    devices = list(devices)
    for device in devices:
        max_value = max(stall_type_distribution_pd[stall_type_distribution_pd["device"] == device]["stall amount"])
        max_amount_each_device[device] = max_value

    # fig.update_yaxes(matches=None)
    # fig.update_layout(yaxis4=dict(range=[0, 10]))
    fig.update_xaxes(mirror=True)
    prettify_the_fig(fig)
    set_title(fig)

    fig.update_xaxes(mirror=True,
                     showline=True)
    fig.update_yaxes(mirror=True,
                     showline=True, showgrid=False)
    fig.write_image("paper_usage/knob_choosing/stall_changing.png")
