# here is an example from online-ml/river

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from feature_selection import generate_lsm_shape
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


def plot_lsm(lsm_shape, plot_level, fig, axes):
    # axes[0].set_title("Compaction Count")
    # colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'w']
    #
    # for i in range(plot_level):
    #     axes[i, 0].plot(lsm_shape["time_micro"], lsm_shape["level" + str(i)], c=colors[i])
    #     axes[i, 0].set_ylabel("level" + str(i))

    return axes


def plot_compaction(compaction_timing_map, lsm_shape, plot_level, fig, axes):
    colors = ['b', 'g', 'c', 'm', 'y', 'k', 'w', 'r']
    fig.suptitle("Number of files in each level")
    ylim_list = [25, 50, 500, 500]

    for i in range(plot_level):
        compaction_ax = axes[i]
        compaction_ax.set_ylabel("level " + str(i))
        lsm_shape_ax = axes[i].twinx()
        lsm_shape_ax.set_yticks([])
        # lsm_shape_ax.scatter(compaction_timing_map[i], [1 for x in compaction_timing_map[i]], c="r")
        compaction_ax.plot(lsm_shape["time_micro"], lsm_shape["level" + str(i)], c=colors[i])
        if i < len(ylim_list):
            compaction_ax.set_ylim(0, ylim_list[i])
    return axes


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (14, 6)
    mpl.rcParams['axes.grid'] = False

    log_dir_prefix = "LOG_DIR/pm_server_fillrandom_3600/"
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        print(log_dir)
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)
        data_set = load_log_and_qps(LOG_file, report_csv)

        lsm_shape = generate_lsm_shape(data_set)
        stat_df = pd.read_csv(stat_csv)

        plot_level = 5

        compaction_timing_map = {x: [] for x in range(plot_level)}
        for index, compaction_job in data_set.compaction_df.iterrows():
            compaction_timing_map[int(compaction_job["output_level"])].append(compaction_job["start_time"] / 1000000)

        fig, axes = plt.subplots(plot_level + 1, 1, sharex='all')

        plot_compaction(compaction_timing_map, lsm_shape, plot_level, fig, axes)

        cumulative_write_bytes = stat_df["disk_write_bytes"]
        mbps = [cumulative_write_bytes[0]]

        for i in range(1, len(cumulative_write_bytes)):
            mbps.append(cumulative_write_bytes[i] - cumulative_write_bytes[i - 1])

        axes[plot_level].plot(data_set.qps_df["secs_elapsed"], data_set.qps_df["interval_qps"])
        axes[plot_level].set_ylabel("IOPS")
        axes[plot_level].set_ylim(0, 400000)
        compaction_timing_ax = axes[plot_level].twinx()

        colors = ['b', 'g', 'c', 'm', 'y', 'k', 'w', 'r']

        for level in range(plot_level):
            timing_points = compaction_timing_map[level]
            compaction_timing_ax.scatter(timing_points, [level + 0.5 for x in timing_points], s=5, c=colors[level],
                                         label="level" + str(level))
            compaction_timing_ax.set_yticks([])
            compaction_timing_ax.set_ylim(0, plot_level)

        # axes[plot_level, 1].plot(mbps)
        # axes[plot_level, 1].set_ylabel("MBPS")
        # axes[plot_level, 1].set_ylim(0,5e8)
        fig.legend()

        output_path = "paper_usage/%s/" % log_dir.replace(log_dir_prefix, "").replace("/", "_")
        mkdir_p(output_path)
        plt.tight_layout()
        plt.savefig("{}/lsm_shape_with_compaction.pdf".format(output_path), bbox_inches="tight")
        plt.savefig("{}/lsm_shape_with_compaction.png".format(output_path), bbox_inches="tight")
        plt.close()
