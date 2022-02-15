# here is an example from online-ml/river
import os.path

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from feature_selection import vectorize_by_compaction_output_level, generate_lsm_shape
from log_class import LogRecorder
from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_and_std_files
from utils.traversal import get_log_dirs


def load_log_and_qps(log_file, ground_truth_csv):
    # load the data
    return LogRecorder(log_file, ground_truth_csv)


def plot_by_cpu_count(dirs, cpu_count=2):
    for log_dir in dirs:
        if str(cpu_count) + "CPU" in log_dir:
            stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)
            report_csv = report_csv[0]
            report_df = pd.read_csv(report_csv)
            bytes_to_GB = 1000 * 1000 * 1000
            bytes_to_MB = 1000 * 1000
            std_info = StdoutReader(stdout_file)
            report_df["live_data_size"] /= bytes_to_GB
            report_df["pending_bytes"] /= bytes_to_GB
            report_df["all_sst_size"] /= bytes_to_GB
            report_df["total_mem_size"] /= bytes_to_MB
            report_df["interval_qps"] /= 1000

            start_time = 800
            end_time = 1000

            report_df["redundant data"] = report_df["all_sst_size"] - report_df["live_data_size"]
            plot_columns = ["interval_qps", "l0_files", "total_mem_size", "pending_bytes"]
            plot_df = report_df[plot_columns][start_time:end_time].round(2)

            device = std_info.device.replace("NVMe", "NVMe ").replace("SATA", "SATA ")
            print(device)
            plot_df.to_csv("csv_results/%d_cpu%s.csv" % (
                int(std_info.cpu_count.replace("CPU", "")),
                device),
                           index=False, sep=" ")
            plot_df.plot(subplots=True)
            device_info_dict[device] = plot_df

    label_list = []

    color_maker_map = {
        "interval_qps": "k", "l0_files": "c--", "total_mem_size": "c--"
    }
    label_column_map = {
        "interval_qps": "Throughput (kOps)",
        "l0_files": "Number of L0 SSTs",
        "total_mem_size": "Memory Component Size (MB)"
    }
    # colors = ["k", "r--", "c-.", "m:", "#ff19c9"]
    ylabel_limit = [(0, 450), (0, 256), (0, 50), (0, 64)]
    plot_units = ["Throughput\nkOps/sec", "Number of\nL0 SSTs", "MB", "GB"]
    if cpu_count == 2:
        plot_labels = ["Throughput", "Number of L0 SSTs"]
        plot_columns = ["interval_qps", "l0_files"]
    else:
        plot_labels = ["Throughput", "Memory Usage"]
        plot_columns = ["interval_qps", "total_mem_size"]
    row_count = len(plot_columns)
    column_count = len(device_info_dict)
    fig, axes = plt.subplots(row_count, column_count, sharex="all", sharey="row")
    # marks = {"interval_qps": "", "Redundancy Overflowing": "md", "Memory Overflowing": "c1"}

    # plot_labels = ["Throughput", "Number of L0 SSTs", "Memory Usage", "Redundant Data Size"]
    for i in range(row_count):
        for j in range(column_count):
            device = list(device_info_dict.keys())[j]
            plot_df = device_info_dict[device]
            if plot_df is not None:
                line, = axes[i, j].plot(plot_df[plot_columns[i]], color_maker_map[plot_columns[i]])
                if i == 0:
                    axes[i, j].set_ylim(ylabel_limit[i])
                    axes[i, j].set_title(device)
                if i == row_count - 1:
                    # axes[i, j].set_xticks([600, 800, 1000])
                    axes[i, j].set_xlabel("time elapsed(sec)")
                    pass
            if j == 0:
                label_list.append(line)
                # metrics_axes = axes[i, j].twinx()
                # axes[i, j].set_ylabel(plot_units[i])
            # axes[i].set_ylim(ylabel_limit[i])
    # plt.xticks(rotation=45)
    # plt.tight_layout()

    plot_labels = []
    for col in plot_columns:
        plot_labels.append(label_column_map[col])
    lgd = fig.legend(label_list,
                     plot_labels,
                     ncol=2, frameon=False,
                     shadow=False)
    fig.subplots_adjust(bottom=0.3)
    fig.savefig("paper_usage/detect_overflows/%d_cpu.pdf" % cpu_count, bbox_inches="tight")
    fig.savefig("paper_usage/detect_overflows/%d_cpu.png" % cpu_count, bbox_inches="tight")


def plot_redundant_overflow(dirs):
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)
        report_csv = report_csv[0]
        report_df = pd.read_csv(report_csv)
        bytes_to_GB = 1000 * 1000 * 1000
        bytes_to_MB = 1000 * 1000
        std_info = StdoutReader(stdout_file)
        report_df["live_data_size"] /= bytes_to_GB
        report_df["pending_bytes"] /= bytes_to_GB
        report_df["all_sst_size"] /= bytes_to_GB
        report_df["total_mem_size"] /= bytes_to_MB
        report_df["interval_qps"] /= 1000

        start_time = 800
        end_time = 1200

        report_df["redundant data"] = report_df["all_sst_size"] - report_df["live_data_size"]
        plot_columns = ["interval_qps", "l0_files", "total_mem_size", "pending_bytes"]
        plot_df = report_df[plot_columns][start_time:end_time].round(2)

        device = std_info.device.replace("NVMe", "NVMe ").replace("SATA", "SATA ")
        print(device)
        plot_df.to_csv("csv_results/%d_cpu%s.csv" % (
            int(std_info.cpu_count.replace("CPU", "")),
            device),
                       index=False, sep=" ")
        plot_df.plot(subplots=True)
        if device in device_info_dict:
            device_info_dict[device] = plot_df

    label_list = []

    color_maker_map = {
        "interval_qps": "k", "l0_files": "k--", "total_mem_size": "c--",
        "redundant data": "c:", "pending_bytes": "c:"
    }
    label_column_map = {
        "interval_qps": "Throughput (kOps)",
        "l0_files": "Number of L0 SSTs",
        "total_mem_size": "Memory Component Size (MB)",
        "redundant data": "Redundant Data Size (GB)",
        "pending_bytes": "Redundant Data Size (GB)"
    }
    # colors = ["k", "r--", "c-.", "m:", "#ff19c9"]
    ylabel_limit = [(0, 450), (0, 256), (0, 50), (0, 64)]
    plot_units = ["Throughput\nkOps/sec", "Number of\nL0 SSTs", "MB", "GB"]

    plot_labels = ["Throughput", "Memory Usage"]
    plot_columns = ["interval_qps", "pending_bytes"]
    row_count = len(plot_columns)
    column_count = len(device_info_dict)
    fig, axes = plt.subplots(row_count, column_count, sharex="all", sharey="row")
    # marks = {"interval_qps": "", "Redundancy Overflowing": "md", "Memory Overflowing": "c1"}

    # plot_labels = ["Throughput", "Number of L0 SSTs", "Memory Usage", "Redundant Data Size"]
    for i in range(row_count):
        for j in range(column_count):
            device = list(device_info_dict.keys())[j]
            plot_df = device_info_dict[device]
            if plot_df is not None:
                line, = axes[i, j].plot(plot_df[plot_columns[i]], color_maker_map[plot_columns[i]])

                if i == 0:
                    axes[i, j].set_ylim(ylabel_limit[i])
                    axes[i, j].set_title(device)
                if i == row_count - 1:
                    # axes[i, j].set_ylim(15, 24)
                    # axes[i, j].set_yticks([15, 20])
                    # axes[i, j].set_xticks([600, 800, 1000])
                    axes[i, j].set_yticks([64])

                    # if j == 0:
                    #     axes[i, j].set_ylabel("Pending Size (GB)")

                    twin_ax = axes[i, j].twinx()
                    l0_line, = twin_ax.plot(plot_df["l0_files"], color_maker_map["l0_files"])
                    twin_ax.set_ylim(15, 30)
                    twin_ax.set_yticks([])
                    if j == column_count - 1:
                        twin_ax.set_yticks([20])
                        # twin_ax.set_ylabel("Number of \nL0 SSTs")

                    axes[i, j].set_xlabel("time elapsed(sec)")
                    pass
            if j == 0:
                label_list.append(line)
                # metrics_axes = axes[i, j].twinx()
                # axes[i, j].set_ylabel(plot_units[i])
            # axes[i].set_ylim(ylabel_limit[i])
    # plt.xticks(rotation=45)
    # plt.tight_layout()

    plot_labels = []
    for col in plot_columns:
        plot_labels.append(label_column_map[col])
    label_list.append(l0_line)
    plt.tight_layout()
    plot_labels.append("Number of L0 Files")
    lgd = fig.legend(label_list,
                     plot_labels,
                     ncol=3, frameon=False,
                     shadow=False)
    fig.subplots_adjust(bottom=0.3)
    fig.savefig("paper_usage/detect_overflows/RO.pdf", bbox_inches="tight")
    fig.savefig("paper_usage/detect_overflows/RO.png", bbox_inches="tight")


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 3)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 12
    mpl.rcParams["legend.loc"] = "lower center"
    mpl.rcParams['lines.markersize'] = 6
    # plot_overflows(2)
    # plot_overflows(20)

    log_dir_prefix = "LOG_DIR/PM_results_redundant_overflow/"

    device_info_dict = {"PM": None, "NVMe SSD": None, }
    # "SATA SSD": None, "SATA HDD": None}

    dirs = get_log_dirs(log_dir_prefix)

    # plot_by_cpu_count(2)
    # plot_by_cpu_count(20)

    plot_redundant_overflow(dirs)
    #
    # log_dir_prefix = "LOG_DIR/PM_results_1200_detail_info/"
    # device_info_dict = {"PM": None, "NVMe SSD": None, "SATA SSD": None, "SATA HDD": None}
    #
    # dirs = get_log_dirs(log_dir_prefix)
    # plot_by_cpu_count(dirs, 2)
    # plot_by_cpu_count(dirs, 20)
