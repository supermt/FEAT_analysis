import matplotlib
import plotly.express as px
from utils.traversal import get_log_dirs, get_log_and_std_files
from utils.stdoutreader import StdoutReader
import pandas as pd
from choosing_knob_thread_impact_on_write_stall import prettify_the_fig
import plotly.graph_objs as go
import matplotlib.pyplot as plt

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

    candidate_thread_no = [1, 4, 12]
    most_frequent_thread = min(candidate_thread_no, key=lambda x: abs(x - most_frequent_thread))

    candidate_batch_size = [64, 128, 256]
    most_frequent_batch = min(candidate_batch_size, key=lambda x: abs(x - most_frequent_batch))
    return most_frequent_batch, most_frequent_thread


def get_stall_duration_and_count(input_dir):
    # input_dir = "LOG_DIR/PM_results_TEAv5_dec_29/"
    stall_count_dict = {}
    stall_duration_dict = {}
    cpu_util_dict = {}

    input_dir = get_log_dirs(input_dir)

    metrics_in_std_files = []
    temp = {}
    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    for log_dir in input_dir:
        stdout_file, LOG_file, report_csv, sys_stat, iostat_files = get_log_and_std_files(log_dir, with_stat_csv=True,
                                                                                          splitted_iostat=True,
                                                                                          multi_tasks=True)
        basic_info = StdoutReader(stdout_file)
        stall_count_dict[basic_info.device] = aggreate_stall_type(basic_info.stall_reasons)
        stall_duration_dict[basic_info.device] = basic_info.stall_duration

        sys_stat_pd = pd.read_csv(sys_stat)
        cpu_utils = sys_stat_pd["cpu_percent"]
        cpu_util_dict[basic_info.device] = [round(cpu_utils.mean(), 1), cpu_utils.max()]

    result_list = []

    for i in range(len(devices)):
        device = devices[i]
        duration = stall_duration_dict[device].split(" ")[0].split(":")
        duration = float(duration[2]) + int(duration[1]) * 60 + int(duration[0]) * 3600

        row = [device, duration]
        row.extend(stall_count_dict[device].values())
        row.extend(cpu_util_dict[device])
        result_list.append(row)
    return result_list


if __name__ == '__main__':
    # result_df = pd.DataFrame(result_list, columns=["Device", "Tuner", "Stall Duration", "LO", "RO", "MO"])
    # result_df.to_csv("csv_results/stall_duration.csv", index=False)
    values = [100, 1000]
    idle_rates = ["0.75", "2"]
    result_list = []
    for value_size in values:
        for idle_rate in idle_rates:
            current_log_dir = "../LOG_DIR/PM_results_FEAT_sensitivity_idle_and_value_size/%dbytes/idle/%s/" % (
                value_size, idle_rate)

            current_list = get_stall_duration_and_count(current_log_dir)
            for row in current_list:
                row.extend([value_size, float(idle_rate)])
            result_list.extend(current_list)

    result_df = pd.DataFrame(result_list,
                             columns=["device", "stall duration(sec)", "LO", "RO", "MO",
                                      "cpu_util_avg", "cpu_util_max",
                                      "value size(bytes)", "idle_rate"])
    result_df = result_df[
        ["device", "value size(bytes)", "idle_rate", "MO", "LO", "RO", "stall duration(sec)", "cpu_util_max",
         "cpu_util_avg"]]
    result_df.to_csv("csv_results/idle_rate_sensitivity.csv", index=False)
