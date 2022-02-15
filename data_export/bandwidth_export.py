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


def count_for_one_case(target_file_dir="", case_name=""):
    log_dir_prefix = target_file_dir
    average_MPBS_pd = []
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        print(log_dir)
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, True)
        print(stat_csv)
        stat_csv_data = pd.read_csv(stat_csv)
        disk_byte_usage = stat_csv_data["syscall_write_bytes"]
        disk_byte_usage /= (1024 * 1024)
        MPBS = disk_byte_usage - disk_byte_usage.shift(1)
        std_result = StdoutReader(stdout_file)
        data_row = [std_result.device, int(std_result.cpu_count.replace("CPU", "")), std_result.batch_size, MPBS.mean(),
                    MPBS.max()]
        average_MPBS_pd.append(data_row)

    needed_threads = [2, 4, 20]

    average_MPBS_pd = pd.DataFrame(average_MPBS_pd, columns=["device", "cpu", "batch size", "avg", "max"])
    average_MPBS_pd = average_MPBS_pd[average_MPBS_pd["cpu"].isin(needed_threads)]

    average_MPBS_pd.to_csv("csv_results/disk_bandwidth/" + case_name + ".csv", index=False)
    pass


if __name__ == '__main__':
    # count_for_one_case("../LOG_DIR/PM_results_DEV_8_nostall_performance/no_restriction_set", "motivation_no_stall")
    # count_for_one_case("../LOG_DIR/PM_results_DOTA_nov_5th/", "DOTA_disk_usage")
    count_for_one_case("../LOG_DIR/PM_results_traversal_1hour/", "origin_data")
