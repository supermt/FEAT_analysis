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
    print(most_frequent_batch, most_frequent_thread)

    candidate_thread_no = [1, 4, 12]
    most_frequent_thread = min(candidate_thread_no, key=lambda x: abs(x - most_frequent_thread))

    candidate_batch_size = [64, 128, 256]
    most_frequent_batch = min(candidate_batch_size, key=lambda x: abs(x - most_frequent_batch))
    return most_frequent_batch, most_frequent_thread


if __name__ == '__main__':

    tuned_dir = "LOG_DIR/PM_results_TEAv5_dec_29/"
    ori_dir = "LOG_DIR/PM_results_traversal_1hour/"
    DOTA_dir = "LOG_DIR/PM_results_DOTA_nov_5th/"

    TEA_dirs = get_log_dirs(tuned_dir)
    ori_dirs = get_log_dirs(ori_dir)
    DOTA_dirs = get_log_dirs(DOTA_dir)

    metrics_in_std_files = []
    temp = {}
    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    default_stall_dict = {}
    TEA_stall_dict = {}
    FEA_stall_dict = {}

    default_stall_duration = {}
    TEA_stall_duration = {}
    FEA_stall_duration = {}

    for log_dir in TEA_dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)
        basic_info = StdoutReader(stdout_file)
        TEA_stall_dict[basic_info.device] = aggreate_stall_type(basic_info.stall_reasons)
        TEA_stall_duration[basic_info.device] = basic_info.stall_duration

    for log_dir in ori_dirs:
        if "64MB" in log_dir and "2CPU" in log_dir:
            stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)
            basic_info = StdoutReader(stdout_file)
            default_stall_dict[basic_info.device] = aggreate_stall_type(basic_info.stall_reasons)
            default_stall_duration[basic_info.device] = basic_info.stall_duration

    for log_dir in DOTA_dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)
        basic_info = StdoutReader(stdout_file)
        FEA_stall_dict[basic_info.device] = aggreate_stall_type(basic_info.stall_reasons)
        FEA_stall_duration[basic_info.device] = basic_info.stall_duration

    Tuners = ["origin", "TEA", "FEAT"]
    target_map = {
        "origin": [default_stall_dict, default_stall_duration],
        "TEA": [TEA_stall_dict, TEA_stall_duration],
        "FEAT": [FEA_stall_dict, FEA_stall_duration]
    }
    result_list = []
    for i in range(len(devices)):
        for j in range(len(Tuners)):
            device = devices[i]
            tuner = Tuners[j]
            duration = target_map[tuner][1][device].split(" ")[0].split(":")
            duration = float(duration[2]) + int(duration[1]) * 60 + int(duration[0]) * 3600

            row = [device, tuner, duration]
            row.extend(target_map[tuner][0][device].values())
            result_list.append(row)
    result_df = pd.DataFrame(result_list, columns=["Device", "Tuner", "Stall Duration", "LO", "RO", "MO"])
    result_df.to_csv("csv_results/stall_duration.csv", index=False)
