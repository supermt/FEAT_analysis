# here is an example from online-ml/river
import os.path

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from feature_selection import vectorize_by_compaction_output_level, generate_lsm_shape
from log_class import LogRecorder
from utils.traversal import get_log_and_std_files
from utils.traversal import get_log_dirs


def load_log_and_qps(log_file, ground_truth_csv):
    # load the data
    return LogRecorder(log_file, ground_truth_csv)


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (10, 6)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16
    mpl.rcParams["legend.loc"] = "lower center"

    log_dir_prefix = "LOG_DIR/PM_results_traversal_1hour/"
    dirs = get_log_dirs(log_dir_prefix)
    key_seq = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]
    default_setting_qps_csv = {x: None for x in key_seq}
    default_setting_lsm_state = {x: None for x in key_seq}
    default_setting_compaction_distribution = {x: None for x in key_seq}
    l0_stall_moments = {x: None for x in key_seq}

    for log_dir in dirs:
        if "2CPU" == log_dir.split(os.sep)[-2] and "64MB" in log_dir:
            print(log_dir)

            stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
            media = log_dir.split(os.sep)[-3].replace("StorageMaterial.", "")
            data_set = load_log_and_qps(LOG_file, report_csv)
            file_states = generate_lsm_shape(data_set)
            bucket_df = vectorize_by_compaction_output_level(data_set, 7)
            default_setting_qps_csv[media] = report_csv
            default_setting_lsm_state[media] = file_states
            l0_stall_moment = file_states[file_states["level0"] >= 20]
            l0_stall_moment = pd.DataFrame(l0_stall_moment, columns=["time_micro", "level0"]).reindex()
            default_setting_compaction_distribution[media] = bucket_df["l0compactions"]
            l0_stall_moments[media] = l0_stall_moment

    material_count = len(default_setting_qps_csv)

    end_time = 2400

    fig, axes = plt.subplots(material_count, 1)
    lines_for_label = []
    for i in range(material_count):
        media = key_seq[i]
        qps_df = pd.read_csv(default_setting_qps_csv[media])
        throughput_line, = axes[i].plot(qps_df["interval_qps"][:end_time])
        axes[i].set_ylim(-100000, 450000)
        axes[i].set_yticks([])
        l0compactions = default_setting_compaction_distribution[media][:end_time]
        event_ax = axes[i].twinx()
        l0_l1_scatter, = event_ax.plot(l0compactions >= 1, "g+")
        l0_stalls = l0_stall_moments[media]
        l0_stalls = l0_stalls[l0_stalls["time_micro"] <= end_time]

        stall_value = l0_stalls["level0"] >= 20
        level_overflowing_scatter, = event_ax.plot(l0_stalls["time_micro"], stall_value * 3, "r.")

        event_ax.set_ylim(0.5, 10)
        event_ax.set_yticks([])
        event_ax.set_xlim(0,end_time+10)
        # temp_ax.set_ylabel("l0 com")
        axes[i].set_title(key_seq[i].replace("SATA","SATA ").replace("NVMe","NVMe "))

    axes[i].set_xlabel("elapsed time (sec)")

    plt.tight_layout()

    # lgd = ax.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, -0.1))
    lgd = fig.legend([throughput_line, l0_l1_scatter, level_overflowing_scatter],
                     ["Throughput", "L0-L1 comp", "L0 overflowing"], ncol=3, fancybox=True, shadow=True)
    fig.subplots_adjust(bottom=0.2)
    plt.savefig("paper_usage/L0-L1_mapping.pdf")
    plt.savefig("paper_usage/L0-L1_mapping.png")
