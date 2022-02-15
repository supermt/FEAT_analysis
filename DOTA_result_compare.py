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


if __name__ == '__main__':

    log_dir_prefix = "LOG_DIR/PM_results_NOV_8_v5/"
    original_log_dir = "LOG_DIR/PM_result_TEA_v1_100value/default_limitation/"
    target_log_dir = "LOG_DIR/pm_server_fillrandom_3600/"

    dirs = get_log_dirs(log_dir_prefix)

    metrics_in_std_files = []
    temp = {}
    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    info_dict = {}
    throughput_dict = {}
    stall_dict = {}

    batch_size_curve = {}
    thread_number_curve = {}

    for log_dir in dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
        basic_info = StdoutReader(stdout_file)
        info_dict[basic_info.device] = basic_info
        # devices.append(basic_info.device)

        metrics_in_std_files.append(basic_info)
        throughput_dict[basic_info.device] = pd.read_csv(report_csv)
        stall_dict[basic_info.device] = aggreate_stall_type(basic_info.stall_reasons)

    throughput_curve_plot = make_subplots(rows=len(devices), cols=3, start_cell="top-left",
                                          subplot_titles=("Default setting throughput", "TEA embedded throughput",
                                                          "Stable configure throughput"))

    stall_comparison_bar = make_subplots(rows=len(devices), cols=1, start_cell="top-left")

    row_count = 0
    for device in devices:
        row_count += 1
        initial_batch_size = 64
        initial_thread_no = 2

        frequent_batch_size, frequent_thread_no = pick_the_most_frequent_set(throughput_dict[device])
        target_file_dir = target_log_dir + "/" + "StorageMaterial." + device + "/" + str(
            frequent_thread_no) + "CPU/" + str(frequent_batch_size) + "MB/"
        initial_file_dir = original_log_dir + "/" + "StorageMaterial." + device + "/" + str(
            initial_thread_no) + "CPU/" + "64MB/"

        init_stdout_file, init_LOG_file, init_report_csv = get_log_and_std_files(initial_file_dir)
        init_throughput = pd.read_csv(init_report_csv)

        target_stdout_file, target_LOG_file, target_report_csv = get_log_and_std_files(target_file_dir)
        target_throughput = pd.read_csv(target_report_csv)

        throughput_curve_plot.update_yaxes(title_text=device, row=row_count, col=1)
        throughput_curve_plot.add_trace(
            go.Scatter(x=init_throughput["secs_elapsed"], y=init_throughput["interval_qps"],
                       line=dict(color=color_map[device]),
                       name=device), row=row_count, col=1)
        throughput_curve_plot.add_trace(
            go.Scatter(x=throughput_dict[device]["secs_elapsed"], y=throughput_dict[device]["interval_qps"],
                       line=dict(color=color_map[device]),
                       name=device), row=row_count, col=2)
        throughput_curve_plot.add_trace(
            go.Scatter(x=target_throughput["secs_elapsed"], y=target_throughput["interval_qps"],
                       line=dict(color=color_map[device]),
                       name=device), row=row_count, col=3)

        stall_comparison_bar.add_trace(go.Bar(x=list(stall_dict[device].keys()),
                                              y=list(stall_dict[device].values()), name=device), row=row_count, col=1)
        original_stalls = aggreate_stall_type(basic_info.stall_reasons)

        stall_comparison_bar.add_trace(go.Bar(x=list(stall_dict[device].keys()),
                                              y=list(stall_dict[device].values()), name=device), row=row_count, col=1)

    throughput_curve_plot.update_yaxes(range=[0, 600000])
    # throughput_curve_plot.show()
    throughput_curve_plot.update_layout(width=1500, height=800)

    prettify_the_fig(throughput_curve_plot)
    throughput_curve_plot.write_image("paper_usage/DOTA_evaluation/throughput_curve.png")

    stall_comparison_bar.write_image("paper_usage/DOTA_evaluation/stall_number.png")

    tuning_step_plot = make_subplots(rows=len(devices), cols=2, start_cell="top-left",
                                     subplot_titles=("Batch Size (MB)", "Thread No."))

    row_count = 0
    for device in devices:
        row_count += 1
        if row_count == 0:
            pass
        tuning_step_plot.add_trace(
            go.Scatter(x=throughput_dict[device]["secs_elapsed"], y=throughput_dict[device]["batch_size"],
                       line=dict(color=color_map[device]),
                       name=device, legendgroup="Batch Size", legendgrouptitle_text="Batch Size"),
            row=row_count, col=1)
        tuning_step_plot.update_yaxes(title_text=device, row=row_count, col=1, range=[0, 550])

        tuning_step_plot.add_trace(
            go.Scatter(x=throughput_dict[device]["secs_elapsed"], y=throughput_dict[device]["thread_num"],
                       name=device,
                       line=dict(color=color_map[device]),
                       # line=dict(dash='dot'),
                       legendgroup="Thread Number",
                       legendgrouptitle_text="Thread No."), row=row_count, col=2)
        tuning_step_plot.update_yaxes(title_text=device, col=2, range=[0, 27], tickvals=[0, 10, 20])

    tuning_step_plot.update_layout(height=500, width=900)
    prettify_the_fig(tuning_step_plot, 16)
    tuning_step_plot.write_image("paper_usage/DOTA_evaluation/tuning_step.png")
