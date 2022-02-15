# here is an example from online-ml/river

import matplotlib as mpl
import matplotlib.pyplot as plt

from feature_selection import *
from log_class import LogRecorder
from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_and_std_files, mkdir_p, get_log_dirs


def load_log_and_qps(log_file, ground_truth_csv):
    # load the data
    return LogRecorder(log_file, ground_truth_csv)


if __name__ == '__main__':
    log_dir_prefix = "../LOG_DIR/PM_serve_results_value_size_changing/1000/"
    plot_features = ["interval_qps", "change_points"]
    key_seq = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]
    cpu_number_list = [2, 4, 20]
    cpu_list = [str(x) + "CPU" for x in cpu_number_list]
    batch_size_list = [str(x) + "MB" for x in [64]]

    # Time,Device, thread_no, batch size, throughput

    dirs = get_log_dirs(log_dir_prefix)

    results_rows = []

    results_dict = {x: {} for x in key_seq}

    for log_dir in dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)
        csv_columns = ["interval_qps", "l0_files", "pending_bytes"]
        csv_df = pd.read_csv(report_csv[0])
        csv_df = csv_df[csv_columns]
        l0_max = csv_df["l0_files"].max()
        l0_min = csv_df["l0_files"].min()
        basic_info = StdoutReader(stdout_file)

        l0_file_seq = list(range(l0_min, l0_max))
        for target_range in l0_file_seq:
            avg_speed = csv_df[csv_df["l0_files"] == target_range].mean()["interval_qps"]
            avg_speed = round(avg_speed, 2)
            entry = results_dict[basic_info.device]
            if target_range in entry:
                entry[target_range].append(avg_speed)
            else:
                entry[target_range] = [avg_speed]
            # results_dict[basic_info.device].append(data_row)

    results_rows = []
    for key in results_dict:
        for l0_file in results_dict[key]:
            avg_speeds = results_dict[key][l0_file]
            results_rows.append([key, l0_file, round(sum(avg_speeds) / len(avg_speeds), 2)])

    plain_pd = pd.DataFrame(results_rows, columns=["device", "l0_file", "throughput"]).to_csv(
        "csv_results/qps_drop/l0_files.csv", index=False, sep=" ")

    # print(plain_pd)
