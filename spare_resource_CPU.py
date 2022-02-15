# here is an example from online-ml/river
import os

import matplotlib as mpl
import matplotlib.pyplot as plt

from feature_selection import *
from log_class import LogRecorder
from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_and_std_files, mkdir_p, get_log_dirs


def load_log_and_qps(log_file, ground_truth_csv):
    # load the data
    return LogRecorder(log_file, ground_truth_csv)


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False
    # plt.rcParams['figure.constrained_layout.use'] = True
    mpl.rcParams['font.size'] = 14

    log_dir_prefix = "LOG_DIR/PM_results_traversal_1hour/"
    plot_features = ["interval_qps", "change_points"]
    # plot_cpu_util(get_log_dirs(log_dir_prefix), log_dir_prefix, "cpu_util/3600/", "cpu_compare", "")
    key_seq = ["PM", "NVMe SSD", "SATA SSD", "SATA HDD"]
    cpu_number_list = [2, 4, 20]
    cpu_list = [str(x) + "CPU" for x in cpu_number_list]
    batch_size_list = [str(x) + "MB" for x in [64]]
    color_list = ["r", "g"]
    alpha_list = [0.5, 0.8]
    row_n = len(key_seq)
    col_n = len(cpu_list)
    fig, axes = plt.subplots(row_n, col_n, sharey=True)
    shared_ax = fig.add_subplot(111, frameon=False)
    shared_ax.tick_params(labelcolor='none', top=False, bottom=False, left=False, right=False)

    dirs = get_log_dirs(log_dir_prefix)
    legend_lines = []
    line_labels = []
    row = 0
    col = 0

    lines = []
    for log_dir in dirs:
        if "64MB" in log_dir:
            stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)
            if log_dir.split(os.sep)[-2] in cpu_list:

                for media in key_seq:
                    if media.replace(" ", "") in log_dir:
                        row = key_seq.index(media)
                for cpu_num in cpu_list:
                    if cpu_num in log_dir:
                        col = cpu_list.index(cpu_num)
                    else:
                        continue
                for batch_size in batch_size_list:
                    if batch_size in log_dir:
                        pass

                thread_num = int(cpu_list[col].replace("CPU", ""))
                if thread_num not in cpu_number_list:
                    continue

                if col == 0:
                    axes[row, col].set_ylabel(key_seq[row])
                if row == 0:
                    axes[row, col].set_title(str(thread_num) + " threads")

                axes[row, col].set_ylim(0, 120)
                stat_df = pd.read_csv(stat_csv)
                line = axes[row, col].plot((stat_df["cpu_percent"][:600] / thread_num))

    # axes[0, 0].add_artist(leg)
    plt.xlabel("elapsed time (sec)")
    # plt.ylabel("CPU utils (%)")
    t = ("CPU Utilization (%)")
    plt.tight_layout()
    plt.text(-0.16, 0.3, t, ha='left', rotation=90, wrap=True)
    print("painting")
    # plt.savefig("paper_usage/12cpu/cpu_utils_in_one_page_all.pdf")
    mkdir_p("paper_usage/knob_choosing/")
    plt.savefig("paper_usage/cpu_utils_in_one_page_all.png")
    plt.savefig("paper_usage/cpu_utils_in_one_page_all.pdf")
