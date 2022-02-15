# here is an example from online-ml/river

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


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (10, 6)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16

    log_dir_prefix = "LOG_DIR/pm_server_fillrandom_3600/"
    dirs = get_log_dirs(log_dir_prefix)
    key_seq = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]
    default_setting_qps_csv = {x: None for x in key_seq}
    default_setting_lsm_state = {x: None for x in key_seq}
    l0_stall_moments = {x: None for x in key_seq}

    time_stop = 1200

    for log_dir in dirs:
        if "1CPU" in log_dir and "64MB" in log_dir:
            stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
            stdout = StdoutReader(stdout_file)
            device = stdout.device
            data_set = load_log_and_qps(LOG_file, report_csv)
            file_states = generate_lsm_shape(data_set)
            l0_stall_moment = file_states[file_states["level0"] >= 20]
            l0_stall_moment = pd.DataFrame(l0_stall_moment, columns=["time_micro", "level0"]).reindex()
            # print(l0_stall_moment)
            # bucket_df = vectorize_by_compaction_output_level(data_set, 7)
            default_setting_qps_csv[device] = report_csv
            # default_setting_lsm_state[media] = file_states
            # pd.read_csv(report_csv).to_csv("csv_results/stall_and_qps/" + device + "_" + "qps.csv", index=False)
            # l0_stall_moment.to_csv("csv_results/stall_and_qps/" + device + "_" + "stall_point.csv", index=False)
            l0_stall_moments[device] = l0_stall_moment
            print(l0_stall_moment)

    material_count = len(default_setting_qps_csv)
    #
    fig, axes = plt.subplots(material_count, 1)
    for i in range(material_count):
        media = key_seq[i]
        qps_df = pd.read_csv(default_setting_qps_csv[media])
        axes[i].plot(qps_df["interval_qps"])
        axes[i].set_ylim(0, 400000)
        l0_stalls = l0_stall_moments[media]
        temp_ax = axes[i].twinx()
        temp_ax.plot(l0_stalls["time_micro"], l0_stalls["level0"] >= 20, "r.")
        temp_ax.set_ylim(0.5,10)
        temp_ax.set_yticks([])
        # temp_ax.set_ylabel("l0 com")
        axes[i].set_title(key_seq[i])
        axes[i].set_ylabel("qps")
    axes[-1].set_xlabel("secs elapsed (sec)")
    plt.tight_layout()
    #
    plt.savefig("paper_usage/stall_dots.png")
    plt.savefig("paper_usage/stall_dots.pdf")

