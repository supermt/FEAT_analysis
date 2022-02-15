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


def generate_time_throughput(stdout_file, stat_csv):
    stat_df = pd.read_csv(stat_csv)
    base_info = StdoutReader(stdout_file)
    result_df = pd.DataFrame(stat_df["cpu_percent"])
    result_df.insert(result_df.shape[1], 'Device', base_info.device)
    result_df.insert(result_df.shape[1], 'Batch Size', base_info.batch_size)
    result_df.insert(result_df.shape[1], 'Thread No.', base_info.cpu_count.replace("CPU", ""))

    return result_df


if __name__ == '__main__':

    log_dir_prefix = "LOG_DIR/pm_server_fillrandom_3600"
    plot_features = ["interval_qps", "change_points"]
    key_seq = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]
    cpu_number_list = [1, 4, 12]
    cpu_list = [str(x) + "CPU" for x in cpu_number_list]
    batch_size_list = [str(x) + "MB" for x in [64, 128]]

    # Time,Device, thread_no, batch size, throughput

    dirs = get_log_dirs(log_dir_prefix)
    legend_lines = []
    line_labels = []
    row = 0
    col = 0
    csv_temp = ["cpu_percent", "Device", "Batch Size", "Thread No."]
    csv_df = pd.DataFrame(columns=csv_temp)
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)
        csv_df = csv_df.append(generate_time_throughput(stdout_file, stat_csv))

    print(csv_df)

    csv_df.to_csv("csv_results/cpu_utils.csv")
