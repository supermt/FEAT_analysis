import plotly.express as px
from utils.traversal import get_log_dirs, get_log_and_std_files
from utils.stdoutreader import StdoutReader
import pandas as pd
from choosing_knob_thread_impact_on_write_stall import prettify_the_fig
import plotly.graph_objs as go
from plotly.subplots import make_subplots

STALL_REASON = ["level0", "pending_compaction_bytes", "memtable"]

color_map = {"PM": "rgb(68,114,196)", "NVMeSSD": "rgb(237,125,49)", "SATASSD": "rgb(165,165,165)",
             "SATAHDD": "rgb(255,192,0)"}


def aggreate_stall_type(stall_dict):
    results = {x: 0 for x in STALL_REASON}
    for key in stall_dict:
        for stall_reason in STALL_REASON:
            if stall_reason in key:
                results[stall_reason] += int(stall_dict[key])
    return results


def stdout_to_dict(stdout_recorder):
    temp_dict = {}
    temp_dict["throughput"] = stdout_recorder.benchmark_results["fillrandom"][1].split(" ")[0]
    temp_dict["threads"] = int(stdout_recorder.cpu_count.replace("CPU", ""))
    temp_dict["batch_size"] = stdout_recorder.batch_size.replace("MB", "")
    temp_dict["device"] = stdout_recorder.device

    temp_dict.update(aggreate_stall_type(stdout_recorder.stall_reasons))

    return temp_dict


def prettify_the_fig(fig, font_size=20):
    fig.update_layout(showlegend=False, font={"size": font_size}, paper_bgcolor='rgba(0,0,0,0)',
                      plot_bgcolor='rgba(0,0,0,0)')
    fig.update_layout(
        margin=go.layout.Margin(
            t=30,
            b=0,  # bottom margin
        )
    )


def pick_the_most_frequent_set(tuning_steps):
    df_mode = tuning_steps.mode()
    most_frequent_thread = int(df_mode["thread_num"][0])
    most_frequent_batch = int(df_mode["batch_size"][0])
    print(most_frequent_batch, most_frequent_thread)

    candidate_thread_no = [1, 4, 12]
    most_frequent_thread = min(candidate_thread_no, key=lambda x: abs(x - most_frequent_thread))

    candidate_batch_size = [64, 128, 256]
    most_frequent_batch = min(candidate_batch_size, key=lambda x: abs(x - most_frequent_batch))
    return most_frequent_batch, most_frequent_thread


def get_value_for_metrics(metrics, workload, device, workloads):
    if metrics == "throughput":
        stdout_obj = workloads[workload]["info_dict"][device]
        return float(stdout_obj.benchmark_results[list(stdout_obj.benchmark_results.keys())[0]][1].replace(" ops/sec",
                                                                                                           ""))
    if metrics == "stall count":
        stall_dict = workloads[workload]["stall_dict"][device]
        return sum(stall_dict.values())
    if metrics == "P99 Latency(read)":
        stdout_obj = workloads[workload]["info_dict"][device]
        return stdout_obj.readrandom_hist["P99"]
    if metrics == "P99 Latency(write)":
        stdout_obj = workloads[workload]["info_dict"][device]
        return stdout_obj.fillrandom_hist["P99"]


if __name__ == '__main__':

    log_dir_prefix = "LOG_DIR/PM_results_DOTA_v5_YCSB/"

    workloads = {"ycsb_a": {}, "ycsb_b": {}}
    workloads_description = {"ycsb_a": "0.5 Read, 0.5 Update", "ycsb_b": "0.95 Read, 0.05 Update"}

    metrics_in_std_files = []
    temp = {}
    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    # for workload in workloads:
    #     workloads[workload] = {"info_dict": {},
    #                            "throughput_dict": {},
    #                            "stall_dict": {}}

    for workload in workloads:
        info_dict = {}
        throughput_dict = {}
        stall_dict = {}

        log_dir_prefix_with_ycsb = log_dir_prefix + workload + "/"
        dirs = get_log_dirs(log_dir_prefix_with_ycsb)

        for log_dir in dirs:
            stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
            basic_info = StdoutReader(stdout_file)
            info_dict[basic_info.device] = basic_info
            # devices.append(basic_info.device)

            metrics_in_std_files.append(basic_info)
            throughput_dict[basic_info.device] = pd.read_csv(report_csv)
            stall_dict[basic_info.device] = aggreate_stall_type(basic_info.stall_reasons)

        workloads[workload]["info_dict"] = info_dict
        workloads[workload]["throughput_dict"] = throughput_dict
        workloads[workload]["stall_dict"] = stall_dict

    throughput_curve_plot = make_subplots(rows=len(devices), cols=len(workloads), start_cell="top-left",
                                          subplot_titles=list(workloads_description.values()))

    stall_comparison_bar = make_subplots(rows=len(devices), cols=1, start_cell="top-left")

    row_count = 0
    for device in devices:
        row_count += 1
        initial_batch_size = 64
        initial_thread_no = 2

        col_num = 0
        for workload in workloads:
            col_num += 1
            throughput_curve_plot.update_yaxes(title_text=device, row=row_count, col=1)
            throughput_curve_plot.add_trace(
                go.Scatter(x=workloads[workload]["throughput_dict"][device]["secs_elapsed"],
                           y=workloads[workload]["throughput_dict"][device]["interval_qps"],
                           line=dict(color=color_map[device]),
                           name=device), row=row_count, col=col_num)
        #
        # stall_comparison_bar.add_trace(go.Bar(x=list(stall_dict[device].keys()),
        #                                       y=list(stall_dict[device].values()), name=device), row=row_count, col=1)
        #
        # stall_comparison_bar.add_trace(go.Bar(x=list(stall_dict[device].keys()),
        #                                       y=list(stall_dict[device].values()), name=device), row=row_count, col=1)

    throughput_curve_plot.update_yaxes(range=[0, 1000000])
    # throughput_curve_plot.show()
    throughput_curve_plot.update_layout(width=1500, height=800)

    prettify_the_fig(throughput_curve_plot)
    throughput_curve_plot.write_image("paper_usage/DOTA_evaluation/YCSB/throughput_curve.png")
    #
    # mixed_figure = make_subplots(rows=3, cols=len(workloads), start_cell="top-left",
    #                              subplot_titles=list(workloads_description.values()))

    paint_df = []
    # columns:
    # workload, device, metrics, value
    metrics_dict = ["throughput", "stall count", "P99 Latency(read)", "P99 Latency(write)"]
    for workload in workloads:
        for device in devices:
            for metrics in metrics_dict:
                data_row = [workload, device, metrics]
                data_row.append(float(get_value_for_metrics(metrics, workload, device, workloads)))
                paint_df.append(data_row)

    paint_df = pd.DataFrame(paint_df, columns=["workload", "device", "metrics", "value"])

    # another pd
    # workload, device, throughput, stall count, P99 Latency read, P99 Latency write
    csv_df = []
    for workload in workloads:
        for device in devices:
            data_row = [workload, device]
            for metrics in metrics_dict:
                data_row.append(float(get_value_for_metrics(metrics, workload, device, workloads)))
            csv_df.append(data_row)
    csv_df = pd.DataFrame(csv_df,
                          columns=["workload", "device", "throughput", "stall count", "P99 Latency read",
                                   "P99 Latency write"])
    csv_df.to_csv("paper_usage/DOTA_evaluation/YCSB/results.csv", index=False)

    # mixed_graph = make_subplots(rows=len(metrics), cols=len(devices), start_cell="top-left", )
    import matplotlib.pyplot as plt
    #
    # mixed_fig, axs = plt.subplots(len(metrics_dict), len(devices))
    #
    # row_count = 0
    # workload_colors = {
    #     "ycsb_a": 'rgb(55, 83, 109)',
    #     "ycsb_b": 'rgb(26, 118, 255)'
    # }
    # import matplotlib as mpl
    #
    # mpl.rcParams['figure.figsize'] = (8, 6)
    # for metric in metrics_dict:
    #     row_count += 1
    #     col_count = 0
    #     for device in devices:
    #         col_count += 1
    #         # Y = paint_df[(paint_df["device"] == device) & (paint_df["metrics"] == metric)]
    #         XX = []
    #         YY = []
    #         for workload in workloads:
    #             XX.append(workload)
    #             Y = paint_df[
    #                 (paint_df["device"] == device) & (paint_df["metrics"] == metric) & (
    #                         paint_df["workload"] == workload)]
    #             YY.append(float(Y["value"]))
    #         print(YY)
    #         axs[row_count - 1, col_count - 1].bar(XX, YY)
    # mixed_fig.tight_layout()
    # plt.show()
    # mixed_graph.write_image("paper_usage/DOTA_evaluation/YCSB/all_in_one.png")
