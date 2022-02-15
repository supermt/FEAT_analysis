# here is an example from online-ml/river

import matplotlib as mpl
import matplotlib.pyplot as plt

from feature_selection import *
from log_class import LogRecorder
from utils.traversal import get_log_and_std_files, mkdir_p, get_log_dirs


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


def plot_cpu_util(dirs, log_prefix, output_prefix, fig_name, condition=""):
    for log_dir in dirs:
        if condition in log_dir:
            stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)

            report_df = read_report_csv_with_change_points(report_csv)
            stat_df = read_stat_csv_new(stat_csv)
            fig, (ax1, ax2) = plt.subplots(2, 1)
            ax1.plot(report_df["secs_elapsed"], report_df["interval_qps"], color="r")
            ax1.set_ylabel("qps")
            ax1.set_ylim(0, 600000)

            ax2.plot(stat_df["secs_elapsed"], stat_df["cpu_utils"], color="b")
            ax2.set_ylabel("cpu_utils")
            ax2.set_ylim(0, 1200)

            plt.tight_layout()
            # report_df[plot_features].plot(subplots=True)
            output_path = output_prefix + "/%s/" % log_dir.replace(log_prefix, "").replace("/", "_")
            mkdir_p(output_path)
            plt.savefig("{}/{}.pdf".format(output_path, fig_name), bbox_inches="tight")
            plt.savefig("{}/{}.png".format(output_path, fig_name), bbox_inches="tight")
            plt.clf()


def plot_stat(dirs, log_prefix, output_prefix, fig_name, condition=""):
    for log_dir in dirs:
        if condition in log_dir:
            stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)

            report_df = read_report_csv_with_change_points(report_csv)
            stat_df = read_stat_csv(stat_csv)
            plt.subplot(311)
            plt.plot(report_df["secs_elapsed"], report_df["interval_qps"], color="r")
            plt.ylabel("qps")
            plt.ylim(0, 600000)

            plt.subplot(312)
            plt.plot(stat_df["secs_elapsed"], stat_df["cpu_utils"], color="b")
            plt.ylabel("cpu_utils")
            plt.plot()
            plt.ylim(0, 1200)

            plt.subplot(313)
            plt.plot(stat_df["secs_elapsed"], stat_df["disk_usage"], color="c")
            # plt.plot(stat_df["secs_elapsed"], [2e7 for x in stat_df["secs_elapsed"]], color="r")
            plt.ylabel("disk_utils")
            # plt.hlines(1e7, 0, stat_df["secs_elapsed"].tolist()[-1], colors="r", linestyles="dashed")
            # plt.hlines(2e7, 0, stat_df["secs_elapsed"].tolist()[-1], colors="g", linestyles="dashed")
            # plt.hlines(3e7, 0, stat_df["secs_elapsed"].tolist()[-1], colors="b", linestyles="dashed")

            # plt.plot()
            #
            # plt.subplot(414)
            # plt.plot(report_df["secs_elapsed"], report_df["change_points"], color="g")
            # plt.ylabel(r"SST Size")
            # plt.ylim(0, 16)

            plt.tight_layout()
            # report_df[plot_features].plot(subplots=True)
            output_path = output_prefix + "/%s/" % log_dir.replace(log_prefix, "").replace("/", "_")
            mkdir_p(output_path)
            plt.savefig("{}/{}.pdf".format(output_path, fig_name), bbox_inches="tight")
            plt.savefig("{}/{}.png".format(output_path, fig_name), bbox_inches="tight")
            plt.clf()


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False
    plt.rcParams['figure.constrained_layout.use'] = True

    log_dir_prefix = "LOG_DIR/pm_server_fillrandom_3600"
    plot_features = ["interval_qps", "change_points"]
    # plot_cpu_util(get_log_dirs(log_dir_prefix), log_dir_prefix, "cpu_util/3600/", "cpu_compare", "")
    key_seq = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]
    cpu_number_list = [1, 4, 12]
    cpu_list = [str(x) + "CPU" for x in cpu_number_list]
    batch_size_list = [str(x) + "MB" for x in [64, 128]]
    color_list = ["r", "g"]
    alpha_list = [0.5, 0.5]
    row_n = len(key_seq)
    col_n = len(cpu_list)
    fig, axes = plt.subplots(row_n, col_n, sharey=True)
    shared_ax = fig.add_subplot(111, frameon=False)
    shared_ax.tick_params(labelcolor='none', top=False, bottom=False, left=False, right=False)
    # Time,Device, thread_no, batch size

    dirs = get_log_dirs(log_dir_prefix)
    legend_lines = []
    line_labels = []
    row = 0
    col = 0
    for log_dir in dirs:
        if "64MB" in log_dir:
            stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)

            for media in key_seq:
                if media in log_dir:
                    row = key_seq.index(media)
            for cpu_num in cpu_list:
                if cpu_num in log_dir:
                    col = cpu_list.index(cpu_num)
            for batch_size in batch_size_list:
                if batch_size in log_dir:
                    color = batch_size_list.index(batch_size)

            axes[row, col].set_title(key_seq[row] + " " + cpu_list[col].replace("CPU", "threads"))
            axes[row, col].set_ylim(0, 1400)
            stat_df = pd.read_csv(stat_csv)

            line_label = "cpu utils (%): " + batch_size_list[color]
            line = axes[row, col].plot(stat_df["cpu_percent"], color=color_list[color], label=line_label,
                                       alpha=alpha_list[color])
            if row == 0 and col == 0:
                legend_lines.append(line)
                line_labels.append(line_label)
                shared_ax.set_ylabel("CPU utils(%)")
                # shared_ax.set_xlabel("Device and number of background threads")
    for cpu_num in cpu_number_list:
        for i in range(row_n):
            col = cpu_number_list.index(cpu_num)
            line = axes[i, col].axhline(cpu_num * 100, linestyle="dashed", label=line_label)

    line_label = "expected utils"
    legend_lines.append(line)
    line_labels.append(line_label)

    fig.legend(legend_lines,  # The line objects
               labels=line_labels,  # The labels for each line
               # loc="center right",  # Position of legend
               bbox_to_anchor=(0.32, 0.96),
               # borderaxespad=0.1,  # Small spacing around legend box
               )

    # plt.tight_layout()
    plt.savefig("paper_usage/cpu_utils_in_one_page_all.pdf")
    plt.savefig("paper_usage/cpu_utils_in_one_page_all.png")
