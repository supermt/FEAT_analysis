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
                      width=1800,
                      height=900
                      )


def set_title(fig):
    fig.update_layout(yaxis7=dict(title="Read Latency (µs)", titlefont=dict(size=FONT_SIZE + 4)))


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


USEFUL_LATENCIES = ["Average", "P99", "Max"]


def aggreate_latency_type(read_latency_dict):
    results = {x: 0 for x in USEFUL_LATENCIES}
    for key in read_latency_dict:
        for stall_reason in USEFUL_LATENCIES:
            if stall_reason in key:
                results[stall_reason] += float(read_latency_dict[key])
    return results


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False

    log_dir_prefix = "LOG_DIR/PM_server_hist_100GB_write_100M_read/"
    latencies_distribution = []
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, True)
        stat_csv_data = pd.read_csv(stat_csv)
        std_result = StdoutReader(stdout_file)
        data_row_head = [std_result.device, std_result.cpu_count.replace("CPU", " "), std_result.batch_size]

        latency_count = aggreate_latency_type(std_result.readrandom_hist)

        for key in latency_count:
            data_row = [x for x in data_row_head]
            data_row.append(key)
            data_row.append(latency_count[key])
            latencies_distribution.append(data_row)

    stall_type_distribution_pd = pd.DataFrame(latencies_distribution,
                                              columns=["device", "thread number", "batch size", "latency type",
                                                       "latency"])
    stall_type_distribution_pd = stall_type_distribution_pd.sort_values(by=["device", "thread number"],
                                                                        ignore_index=True)
    print(stall_type_distribution_pd)

    stall_type_distribution_pd.to_csv("paper_usage/knob_choosing/read_latency.csv")

    import plotly.express as px

    devices = stall_type_distribution_pd["device"].unique()
    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]
    #
    fig = px.bar(stall_type_distribution_pd, x="thread number", facet_row="device", facet_col="latency type",
                 y="latency", barmode="group",
                 color="batch size", category_orders={
            "device": devices, "batch size": ["64MB", "128MB"], "latency type": USEFUL_LATENCIES,
            "thread number": ["1 ", "4 ", "20 "]
        }, labels={"device": "", "latency": ""})
    max_amount_each_device = {}

    fig.update_traces(textposition="outside", textangle=90,
                      texttemplate='%{y:.2f}')

    fig.update_layout(uniformtext_minsize=16)

    fig.update_xaxes(mirror=True)
    fig.update_yaxes(matches=None)

    row_n = len(devices)
    col_n = len(USEFUL_LATENCIES)

    col_range = [50, 150, 10000]

    for col in range(2):
        for row in range(row_n):
            fig.update_yaxes(range=[0, col_range[col]], col=col + 1, row=row + 1)
    for row in range(row_n):
        fig.update_yaxes()

    prettify_the_fig(fig)
    set_title(fig)

    fig.update_xaxes(mirror=True,
                     showline=True, tickmode='array',
                     # tickvals=[1, 4, 12]
                     )
    fig.update_yaxes(mirror=True,
                     showline=True, showgrid=False, tickvals=[])
    fig.write_image("paper_usage/knob_choosing/batch/impact_on_read_latency.png")
