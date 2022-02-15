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


def get_avg_speed_by_memory(csv_df, mem_size):
    csv_rows = (csv_df["total_mem_size"] >= mem_size - gap) & (csv_df["total_mem_size"] < mem_size)
    return csv_df[csv_rows]


if __name__ == '__main__':
    log_dir_prefix = "../LOG_DIR/PM_serve_results_value_size_changing/1000/"
    plot_features = ["interval_qps", "change_points"]
    key_seq = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]
    cpu_number_list = [2]
    cpu_list = [str(x) + "CPU" for x in cpu_number_list]
    batch_size_list = [str(x) + "MB" for x in [64]]

    # Time,Device, thread_no, batch size, throughput

    dirs = get_log_dirs(log_dir_prefix)

    results_rows = []

    for log_dir in dirs:
        for cpu_number in cpu_list:
            if cpu_number in log_dir:
                stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True,
                                                                                    multi_tasks=True)
                csv_columns = ["secs_elapsed", "interval_qps", "total_mem_size", "l0_files", "pending_bytes"]

                csv_df = pd.read_csv(report_csv[0])
                csv_df["interval_qps"] = csv_df["interval_qps"]
                csv_df = csv_df[csv_columns]
                basic_info = StdoutReader(stdout_file)
                print(basic_info.device, csv_df)

                csv_df["total_mem_size"] /= (1024 * 1024)
                # csv_df["interval_qps"] = csv_df["interval_qps"].shift()

                mem_range = csv_df["total_mem_size"].max()
                gap = 5
                Memory_threshold = range(gap, int(mem_range), gap)
                for memory in Memory_threshold:
                    speed_range = get_avg_speed_by_memory(csv_df, memory)["interval_qps"].mean()

                    data_row = [basic_info.device.replace("NVMe", "NVMe ").replace("SATA", "SATA "),
                                basic_info.cpu_count.replace("CPU", ""),
                                basic_info.batch_size, memory, speed_range]
                    results_rows.append(data_row)

    pd.DataFrame(results_rows, columns=["device", "thread", "batch_size", "memory", "throughput"]).to_csv(
        "csv_results/qps_drop/mmo.csv", index=False, sep=" ")
