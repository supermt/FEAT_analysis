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

PM_device_mapper = {
    "SATAHDD": "sdc",
    "SATASSD": "sda",
    "NVMeSSD": "nvme0n1",
    "PM": "pmem0"
}

IOSTAT_COLUMN_NAMES = "Device             tps    MB_read/s    MB_wrtn/s    MB_read    MB_wrtn"
IOSTAT_COLUMN = IOSTAT_COLUMN_NAMES.split()


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
        stdout_file, LOG_file, report_csv, stat_csv, iostat_text = get_log_and_std_files(log_dir, True, True)

        std_result = StdoutReader(stdout_file)
        iostat_lines = open(iostat_text, "r", encoding="utf-8").readlines()
        IOSTAT = []
        for line in iostat_lines:
            if PM_device_mapper[std_result.device] in line:
                IOSTAT.append(line.split())
        IOSTAT = pd.DataFrame(IOSTAT, columns=IOSTAT_COLUMN)
        MBPS = IOSTAT["MB_wrtn/s"].astype(float)

        data_row = [std_result.device, int(std_result.cpu_count.replace("CPU", "")), std_result.batch_size,
                    round(MBPS.mean(), 2), MBPS.max()]
        average_MPBS_pd.append(data_row)

    average_MPBS_pd = pd.DataFrame(average_MPBS_pd, columns=["device", "cpu", "batch size", "avg", "max"])
    average_MPBS_pd.to_csv("csv_results/disk_bandwidth/IO_STAT/" + case_name + ".csv", index=False)
    pass


if __name__ == '__main__':
    count_for_one_case("../LOG_DIR/PM_result_iostat/no_restriction_set", "motivation_no_stall")
    count_for_one_case("../LOG_DIR/PM_result_iostat/default", "origin_data")
    # count_for_one_case("../LOG_DIR/PM_results_DOTA_IOSTAT/1200", "DOTA_short_performance")
    # count_for_one_case("../LOG_DIR/PM_results_DOTA_IOSTAT/3600", "DOTA_long_term_performance")
