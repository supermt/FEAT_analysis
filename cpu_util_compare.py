# here is an example from online-ml/river

import matplotlib.pyplot as plt

from feature_selection import *
from log_class import LogRecorder
from utils.traversal import get_log_and_std_files, mkdir_p
from utils.traversal import get_log_dirs


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
            print(log_dir)
            stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)

            report_df = read_report_csv_with_change_points(report_csv)
            stat_df = read_stat_csv_new(stat_csv)
            fig,(ax1,ax2) = plt.subplots(2,1)
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
            print(log_dir)
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
    # log_dir_prefix = "fillrandom_only_thread/"
    # plot_features = ["interval_qps", "change_points"]
    # plot_stat(get_log_dirs(log_dir_prefix), log_dir_prefix, "thread_pri/ori", "Disk_util", "12CPU")
    # log_dir_prefix = "fillrandom_pri_L1_Deep_L0/"
    # plot_features = ["interval_qps", "change_points"]
    # plot_stat(get_log_dirs(log_dir_prefix), log_dir_prefix, "thread_pri/new", "Disk_util", "12CPU")
    # log_dir_prefix = "fillrandom_universal_compaction/"
    # plot_features = ["interval_qps", "change_points"]
    # plot_stat(get_log_dirs(log_dir_prefix), log_dir_prefix, "compaction_style/universal", "Disk_util", "12CPU")
    # log_dir_prefix = "entry_size_test/16/"
    # plot_features = ["interval_qps", "change_points"]
    # plot_stat(get_log_dirs(log_dir_prefix), log_dir_prefix, "entry_size_test/images/universal/8+16", "Disk_util", "12CPU")
    log_dir_prefix = "LOG_DIR/fillrandom_baseline_1200/"
    plot_features = ["interval_qps", "change_points"]
    plot_cpu_util(get_log_dirs(log_dir_prefix), log_dir_prefix, "image_result_fillrandom/1200/", "cpu_compare", "")

    # log_dir_prefix = "DOTA_embedded/"
    # plot_stat(get_log_dirs(log_dir_prefix), log_dir_prefix, "CPU_compare", "QPS_change_point",
    #           "12CPU")
