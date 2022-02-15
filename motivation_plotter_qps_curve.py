# here is an example from online-ml/river
import os

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from log_class import LogRecorder
from utils.traversal import get_log_and_std_files
from utils.traversal import get_log_dirs


def load_log_and_qps(log_file, ground_truth_csv):
    # load the data
    return LogRecorder(log_file, ground_truth_csv)


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (12, 10)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16

    log_dir_prefix = "LOG_DIR/pm_server_rate_limiting/"
    dirs = get_log_dirs(log_dir_prefix)
    key_seq = ["PM", "NVMe SSD", "SATA SSD", "SATA HDD"]
    media_list = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]
    media_dict = dict(zip(media_list, key_seq))
    rate_list = ['5000000', '15000000', '30000000', "NoLimit"]
    col_num = len(rate_list)

    default_setting_qps_csv = {x: None for x in key_seq}
    material_count = len(default_setting_qps_csv)

    fig, axes = plt.subplots(material_count, col_num, sharey=True, sharex=True)

    for col in range(col_num):
        default_setting_qps_csv = {x: None for x in key_seq}
        for log_dir in dirs:
            if "1CPU" in log_dir and "64MB" in log_dir and rate_list[col] == log_dir.split(os.sep)[-4]:
                stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
                media = log_dir.split(os.sep)[-3].replace("StorageMaterial.", "")
                media = media_dict[media]
                default_setting_qps_csv[media] = report_csv
                print(report_csv)

        for i in range(material_count):
            print(i, col)
            media = key_seq[i]
            qps_df = pd.read_csv(default_setting_qps_csv[media])
            qps_df = qps_df[qps_df["secs_elapsed"] < 600]
            axes[i, col].plot(qps_df["interval_qps"] / 1000)
            axes[i, col].set_ylim(0, 450)
            if rate_list[col] != "NoLimit":
                throughput_mbps = int(rate_list[col]) / 1000000
                fig_title = key_seq[i] + "\n" + str(throughput_mbps) + " MB/s"
            else:
                throughput_mbps = "No Limit"
                fig_title = key_seq[i] + "\n" + str(throughput_mbps)
            axes[i, col].set_title(fig_title)
            if col == 0:
                axes[i, col].set_ylabel("kOps/sec", fontsize=20)
            if i == 3:
                axes[i, col].set_xlabel("elapsed time (sec)", fontsize=20)
            plt.tight_layout()

            plt.savefig("paper_usage/motivation-rate-limiting-qps-600.pdf")
            plt.savefig("paper_usage/motivation-rate-limiting-qps-600.png")
