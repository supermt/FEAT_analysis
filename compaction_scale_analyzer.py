# here is an example from online-ml/river

import matplotlib as mpl
import matplotlib.pyplot as plt

from feature_selection import *
from log_class import LogRecorder
from utils.traversal import get_log_and_std_files, mkdir_p
from utils.traversal import get_log_dirs


def load_log_and_qps(log_file, ground_truth_csv):
    # load the data
    return LogRecorder(log_file, ground_truth_csv)


def plot_compaction(compaction_df, plot_level, fig, axes, col=0):
    axes[0, col].set_title("compaction jobs")
    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'w']
    for i in range(plot_level):
        axes[i, col].plot(compaction_df["level" + str(i)], c=colors[i])
        axes[i, 0].set_ylabel("level" + str(i))

    return axes


def plot_write_per_level(compaction_df, plot_level, fig, axes, col=1):
    axes[0, col].set_title("Output entry number")
    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'w']
    for i in range(plot_level):
        axes[i, col].plot(compaction_df["output_in_level" + str(i)], c=colors[i])
        # axes[i, 0].set_ylabel("level" + str(i))

    return axes


def plot_read_per_level(compaction_df, plot_level, fig, axes, col=2):
    axes[0, col].set_title("Input entry number")
    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'w']
    for i in range(plot_level):
        axes[i, col].plot(compaction_df["input_in_level" + str(i)], c=colors[i])
        # axes[i, 0].set_ylabel("level" + str(i))

    return axes


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False

    log_dir_prefix = "LOG_DIR/fillrandom_log/"
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        print(log_dir)
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
        data_set = load_log_and_qps(LOG_file, report_csv)
        lsm_shape = generate_lsm_shape(data_set)
        plot_level = 4
        compaction_df = vectorize_compaction_scale_each_lvl(data_set, plot_level)

        fig, axes = plt.subplots(plot_level + 1, 3, sharex='all')

        plot_compaction(compaction_df, plot_level, fig, axes)
        plot_read_per_level(compaction_df, plot_level, fig, axes,1)
        plot_write_per_level(compaction_df, plot_level, fig, axes,2)

        axes[plot_level, 0].plot(data_set.qps_df["secs_elapsed"], data_set.qps_df["interval_qps"])
        axes[plot_level, 0].set_ylabel("IOPS")

        axes[plot_level, 1].plot(compaction_df["read"])
        axes[plot_level, 1].set_ylabel("MBPS(R)")

        axes[plot_level, 2].plot(compaction_df["write"])
        axes[plot_level, 2].set_ylabel("MBPS(W)")

        output_path = "compaction_style/level/%s/" % log_dir.replace(log_dir_prefix, "").replace("/", "_")
        mkdir_p(output_path)
        plt.tight_layout()
        plt.savefig("{}/compaction_scale.pdf".format(output_path), bbox_inches="tight")
        plt.savefig("{}/compaction_scale.png".format(output_path), bbox_inches="tight")
        plt.close()
