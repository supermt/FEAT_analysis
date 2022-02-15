import matplotlib.pyplot as plt
import plotly.graph_objs as go

from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_dirs, get_log_and_std_files, mkdir_p


def prettify_the_fig(fig):
    fontsize = 14
    fig.update_layout(legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.1,
        xanchor="left",
        x=0.25,
        font=dict(size=fontsize + 4)))

    fig.update_layout(font=dict(size=fontsize),
                      margin=go.layout.Margin(
                          l=0,  # left margin
                          r=10,  # right margin
                          b=0,  # bottom margin
                          t=0  # top margin
                      )
                      )


if __name__ == '__main__':
    tradeoff_data = []

    log_dir_prefix = "LOG_DIR/PM_server_results_256M_write_10M_read/"
    dirs = get_log_dirs(log_dir_prefix)

    metrics_in_std_files = []
    for log_dir in dirs:
        # if "1CPU" in log_dir:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
        metrics_in_std_files.append(StdoutReader(stdout_file))

    result_dict_list = []
    filed_names = []

    stall_reasons = []

    for std_record in metrics_in_std_files:
        temp_dict = {}
        temp_dict["threads"] = std_record.cpu_count.replace("CPU", " threads")
        temp_dict["material"] = std_record.device
        temp_dict["batch_size"] = std_record.batch_size
        temp_dict["fillrandom_speed"] = std_record.benchmark_results["fillrandom"][1].split(" ")[0]
        temp_dict["readrandom_speed"] = std_record.benchmark_results["readrandom"][1].split(" ")[0]
        temp_dict.update(std_record.tradeoff_data)
        for level in std_record.read_latency_map:
            for level_entry in std_record.read_latency_map[level]:
                temp_dict["read_latency_" + level + "_" + level_entry] = std_record.read_latency_map[level][level_entry]
        stall_reasons.extend(std_record.stall_reasons.keys())
        result_dict_list.append(temp_dict)

    stall_reasons = list(set(stall_reasons))
    filed_names = temp_dict.keys()

    import csv

    csv_file_name = "csv_results/tradeoff_analysis_pm_server.csv"
    try:
        with open(csv_file_name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=filed_names)
            writer.writeheader()
            for data_line in result_dict_list:
                writer.writerow(data_line)
    except IOError:
        print("I/O error")

    field_list = list(filed_names)
    pk_list = field_list[:3]
    import pandas as pd

    paint_df = pd.read_csv(csv_file_name)

    temp_df = paint_df[field_list[:4]]
    color = temp_df["batch_size"].unique()

    plt.rcParams['figure.figsize'] = (8, 4)
    plt.rcParams['axes.grid'] = False
    row = temp_df["material"].unique()
    column = temp_df["threads"].unique()
    fig, axes = plt.subplots(len(row), len(column), sharey=True)

    import plotly.express as px

    tradeoff_dir_prefix = "paper_usage/tradeoff_analysis/same_running_time/"
    mkdir_p(tradeoff_dir_prefix)

    cat_order = {"material": ["PM", "NVMeSSD", "SATASSD", "SATAHDD"], "batch_size": ["64MB", "128MB", "512MB"],
                 "threads": ["1 threads", "4 threads", "12 threads"]}

    stall_pk_list = list(pk_list)
    stall_pk_list.extend(stall_reasons)
    stall_difference_df = paint_df[stall_pk_list]
    stall_paint_df = []
    paint_stall_reason = []
    replaced_name = {"level0_slowdown": "l0 slow", "level0_slowdown_with_compaction": "l0 stop",
                     "memtable_compaction": "memtable stop"}

    for reason in stall_reasons:
        stall_count = sum(stall_difference_df[reason])
        if stall_count != 0:
            paint_stall_reason.append(reason)

    for reason in paint_stall_reason:
        for index, line in stall_difference_df.iterrows():
            temp = line[pk_list].tolist()
            temp.append(replaced_name[reason])
            temp.append(line[reason])

            stall_paint_df.append(temp)

    stall_paint_df = pd.DataFrame(stall_paint_df, columns=["threads", "material", "batch_size", "reason", "count"])
    import plotly.graph_objects as go

    performance_comparison_fig = px.bar(stall_paint_df, x="batch_size",
                                        y="count", color="material", barmode="group", facet_col="threads",
                                        facet_row="reason"
                                        , category_orders=cat_order)
#                                            subplot_titles=[x + " count" for x in paint_stall_reason])
# line_colors = ["red", 'blue', 'dimgray', "deepskyblue"]
#

# for material in cat_order["material"]:
#     for thread_count in cat_order["threads"]:
#         for reason in paint_stall_reason:
#             performance_comparison_fig.append_trace(go.Scatter(x=cat_order["batch_size"], y=stall_paint_df[
#                 (stall_paint_df["material"] == material)
#                 & (stall_paint_df["threads"] == thread_count)
#                 & (stall_paint_df["reason"] == reason)
#                 ]["count"], line_color=line_colors[cat_order["material"].index(material)],
#                                                                name=material + thread_count
#                                                                , legendgroup=material,
#                                                                showlegend=(paint_stall_reason.index(
#                                                                    reason)) == 0),
#                                                     row=paint_stall_reason.index(reason) + 1, col=1)

performance_comparison_fig.write_image("paper_usage/write_stall_tendency/overview.png")
