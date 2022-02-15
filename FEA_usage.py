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

    TEA_only_dir = "LOG_DIR/FEAT_600_test/TEA"

    dirs = get_log_dirs(TEA_only_dir)

    metrics_in_std_files = []
    temp = {}
    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]

    info_dict = {}
    TEA_report_dict = {}
    stall_dict = {}

    batch_size_curve = {}
    thread_number_curve = {}

    for log_dir in dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)

        basic_info = StdoutReader(stdout_file)
        info_dict[basic_info.device] = basic_info
        # devices.append(basic_info.device)

        qps_df = pd.read_csv(report_csv[0])
        time_gap = qps_df["secs_elapsed"] - qps_df["secs_elapsed"].shift(1).fillna(0)
        qps_df["interval_qps"] /= time_gap
        TEA_report_dict[basic_info.device] = qps_df

    FEA_log_dir = "LOG_DIR/FEAT_600_test/FEAT"
    FEA_report_dict = {}
    FEA_dirs = get_log_dirs(FEA_log_dir)

    for fea_dir in FEA_dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(fea_dir, multi_tasks=True)
        basic_info = StdoutReader(stdout_file)
        device = basic_info.device
        info_dict[basic_info.device] = basic_info

        qps_df = pd.read_csv(report_csv[0])
        qps_df = qps_df[qps_df["secs_elapsed"] <= 1200]
        FEA_report_dict[basic_info.device] = qps_df

    fig, axes = plt.subplots(len(devices), 2)
    for i in range(len(devices)):
        device = devices[i]
        tea_knobs = TEA_report_dict[device]
        fea_knobs = FEA_report_dict[device]
        line_default, = axes[i, 0].plot(tea_knobs["secs_elapsed"], tea_knobs["thread_num"], "r--", alpha=0.5)
        axes[i, 1].plot(tea_knobs["secs_elapsed"], tea_knobs["batch_size"], "r--", alpha=0.5)

        axes[i, 0].plot(fea_knobs["secs_elapsed"], fea_knobs["thread_num"], "b", alpha=0.5)
        line_FEAT, = axes[i, 1].plot(fea_knobs["secs_elapsed"], fea_knobs["batch_size"], "b", alpha=0.5)

        temp_ax = axes[i, 1].twinx()
        temp_ax.set_ylabel(device.replace("NVMe", "NVMe ").replace("SATA", "SATA "), rotation=270, labelpad=10)
        temp_ax.set_yticks([])

        axes[i, 0].set_ylim(0, 32)
        axes[i, 1].set_ylim(0, 550)
        # temp_ax.set_ylabel("l0 com")

    axes[0, 0].set_title("Number of Threads")
    axes[0, 1].set_title("Batch Size (MB)")
    label_list = [line_default, line_FEAT]
    fig.set_size_inches(6, 4.5, forward=True)
    axes[3, 0].set_xlabel("elapsed time (sec)", fontsize=12)
    axes[3, 1].set_xlabel("elapsed time (sec)", fontsize=12)
    plt.tight_layout()

    print(label_list)
    # mpl.rcParams["legend.loc"] = "lower center"

    lgd = fig.legend(label_list,
                     ["TEA only", "FEAT (FEA + TEA)", ],
                     #  "Data overflowing", "Redundancy overflowing",                      "Memory overflowing"],
                     ncol=3, fancybox=True,
                     shadow=True, loc="lower center")
    fig.subplots_adjust(bottom=0.2)

    fig.savefig("paper_usage/FEAT_integrate600.png")
    fig.savefig("paper_usage/FEAT_integrate600.pdf")
