# here is an example from online-ml/river
from turtle import color

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from feature_selection import vectorize_by_disk_op_distribution, combine_vector_with_qps
from log_class import LogRecorder
from utils.stdoutreader import StdoutReader
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


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False

    log_dir_prefix = "LOG_DIR/PM_results_choosing_knobs_cpu_utils_multi_flushing/"
    average_MPBS_pd = []
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:

        print(log_dir)
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, True)
        stat_csv_data = pd.read_csv(stat_csv)
        disk_byte_usage = stat_csv_data["disk_write_bytes"]
        disk_byte_usage /= (1024 * 1024)
        MPBS = disk_byte_usage - disk_byte_usage.shift(1)
        std_result = StdoutReader(stdout_file)
        # plt.plot(disk_byte_usage - disk_byte_usage.shift(1))
        # fig.show()
        if std_result.cpu_count == "12CPU":
            data_row = [std_result.device, std_result.cpu_count, std_result.batch_size, MPBS.mean(), MPBS.max()]
            average_MPBS_pd.append(data_row)
        # fig = plt.figure(figsize=(8,6))
        # output_path = "paper_usage/choosing_knobs/%s/" % log_dir.replace(log_dir_prefix, "").replace("/", "_")
        # mkdir_p(output_path)
        # plt.savefig("{}/disk_usage.pdf".format(output_path), bbox_inches="tight")
        # plt.savefig("{}/disk_usage.png".format(output_path), bbox_inches="tight")
        # plt.close()

    average_MPBS_pd = pd.DataFrame(average_MPBS_pd, columns=["device", "cpu", "batch size", "avg","max"])

    average_MPBS_pd.to_csv("paper_usage/knob_choosing/disk_usage.csv", index=False)
