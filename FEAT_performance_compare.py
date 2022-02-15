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

    TEA_dir = "LOG_DIR/FEAT_3600_test/TEA/"
    ori_dir = "LOG_DIR/FEAT_3600_test/default/"
    DOTA_dir = "LOG_DIR/FEAT_3600_test/FEAT/"

    TEA_dirs = get_log_dirs(TEA_dir)
    ori_dirs = get_log_dirs(ori_dir)
    DOTA_dirs = get_log_dirs(DOTA_dir)

    metrics_in_std_files = []
    temp = {}
    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    TEA_throughput_dict = {}
    origin_throughput_dict = {}
    DOTA_throughput_dict = {}
    stall_dict = {}

    avg_speed_dict = {"TEA": {}, "FEAT": {}, "Default": {}}

    for log_dir in TEA_dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)

        basic_info = StdoutReader(stdout_file)

        qps_df = pd.read_csv(report_csv[0])
        time_gap = qps_df["secs_elapsed"] - qps_df["secs_elapsed"].shift(1).fillna(0)
        qps_df["interval_qps"] /= time_gap
        TEA_throughput_dict[basic_info.device] = qps_df

    for log_dir in ori_dirs:
        if "64MB" in log_dir and "2CPU" in log_dir:
            stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)

            basic_info = StdoutReader(stdout_file)

            qps_df = pd.read_csv(report_csv[0])
            time_gap = qps_df["secs_elapsed"] - qps_df["secs_elapsed"].shift(1).fillna(0)
            qps_df["interval_qps"] /= time_gap
            origin_throughput_dict[basic_info.device] = qps_df

    for log_dir in DOTA_dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)

        basic_info = StdoutReader(stdout_file)

        qps_df = pd.read_csv(report_csv[0])
        time_gap = qps_df["secs_elapsed"] - qps_df["secs_elapsed"].shift(1).fillna(0)
        qps_df["interval_qps"] /= time_gap
        DOTA_throughput_dict[basic_info.device] = qps_df

    fig, axes = plt.subplots(len(devices), 3, sharex=True, sharey="row")
    stop_time = 600

    for i in range(len(devices)):
        device = devices[i]
        TEA_qps_df = TEA_throughput_dict[device]
        TEA_qps_df = TEA_qps_df[TEA_qps_df["secs_elapsed"] <= stop_time]
        TEA_avg = TEA_qps_df["interval_qps"].mean() / 1000

        ori_qps_df = origin_throughput_dict[device]
        ori_qps_df = ori_qps_df[ori_qps_df["secs_elapsed"] <= stop_time]
        ori_avg = ori_qps_df["interval_qps"].mean() / 1000

        DOTA_qps_df = DOTA_throughput_dict[device]
        DOTA_qps_df = DOTA_qps_df[DOTA_qps_df["secs_elapsed"] <= stop_time]
        DOTA_avg = DOTA_qps_df["interval_qps"].mean() / 1000

        DOTA_qps_df["interval_qps"] /= 1000
        TEA_qps_df["interval_qps"] /= 1000
        ori_qps_df["interval_qps"] /= 1000

        axes[i, 1].plot(TEA_qps_df["secs_elapsed"], TEA_qps_df["interval_qps"])
        axes[i, 0].plot(ori_qps_df["secs_elapsed"], ori_qps_df["interval_qps"])
        instantaneous_qps, = axes[i, 2].plot(DOTA_qps_df["secs_elapsed"], DOTA_qps_df["interval_qps"])

        axes[i, 1].plot(TEA_qps_df["secs_elapsed"], [TEA_avg] * len(TEA_qps_df), "r--", )
        axes[i, 0].plot(ori_qps_df["secs_elapsed"], [ori_avg] * len(ori_qps_df),
                        "r--", )
        avg_qps, = axes[i, 2].plot(DOTA_qps_df["secs_elapsed"],
                                   [DOTA_avg] * len(DOTA_qps_df), "r--")

        axes[i, 0].set_ylim(0, 450)
        axes[i, 0].set_yticks([300])
        axes[i, 1].set_ylim(0, 450)
        axes[i, 1].set_yticks([300])
        axes[i, 2].set_ylim(0, 450)
        axes[i, 2].set_yticks([300])
        # temp_ax.set_ylabel("l0 com")
        tempax = axes[i, 2].twinx()
        tempax.set_ylabel(device.replace("NVMe", "NVMe ").replace("SATA", "SATA "), rotation=270, labelpad=10)
        tempax.set_yticks([])

    axes[2, 0].set_ylabel("System Throught (kOps/Sec)")
    axes[0, 0].set_title("Default Setting")
    axes[0, 1].set_title("TEA only")
    axes[0, 2].set_title("FEAT (FEA+TEA)")
    axes[3, 0].set_xlabel("elapsed time (sec)", fontsize=12)
    axes[3, 1].set_xlabel("elapsed time (sec)", fontsize=12)
    axes[3, 2].set_xlabel("elapsed time (sec)", fontsize=12)
    # plt.xlabel("secs elapsed (sec)")
    fig.set_size_inches(6, 4.5, forward=True)
    plt.tight_layout()

    lgd = fig.legend([instantaneous_qps, avg_qps],
                     ["Instantaneous Throughput", "Average Throughput", ],
                     #  "Data overflowing", "Redundancy overflowing",                      "Memory overflowing"],
                     ncol=3, fancybox=True,
                     shadow=True, loc="lower center")
    fig.subplots_adjust(bottom=0.2)

    fig.savefig("paper_usage/performance_compare_TEA600.png")
    fig.savefig("paper_usage/performance_compare_TEA600.pdf")
