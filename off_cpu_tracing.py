# here is an example from online-ml/river

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

from log_class import LogRecorder
from utils.traversal import get_log_and_std_files, mkdir_p
from utils.traversal import get_log_dirs
from utils.stdoutreader import StdoutReader


def load_log_and_qps(log_file, ground_truth_csv):
    # load the data
    return LogRecorder(log_file, ground_truth_csv, iostat=True)


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (14, 6)
    mpl.rcParams['axes.grid'] = False
    mpl.rcParams['font.size'] = 16

    log_dir_prefix = "LOG_DIR/pm_result_detailed_perf/"
    dirs = get_log_dirs(log_dir_prefix)
    result_list = []
    prettify_result_list = []

    io_stats_feature = ["file_write_nanos", "file_range_sync_nanos", "file_fsync_nanos", "file_prepare_write_nanos"]
    output_path = "paper_usage/off_cpu/io_stats/"
    # io_stats_feature.append("total_output_size")
    materials = ["StorageMaterial." + x for x in ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]]
    thread_counts = [str(x) + "CPU" for x in [1, 4, 12]]
    batch_size_list = ["64MB", "128MB"]
    for media in materials:
        for thread_count in thread_counts:
            for batch_size in batch_size_list:
                log_dir = log_dir_prefix + "%s/%s/%s" % (media, thread_count, batch_size)
                print(log_dir)
                stdout_file, LOG_file, report_csv, stat_csv = get_log_and_std_files(log_dir, with_stat_csv=True)
                data_set = load_log_and_qps(LOG_file, report_csv)
                plot_features = [x for x in io_stats_feature]
                plot_features.append("total_output_size")
                std_reporter = StdoutReader(stdout_file)

                ns_to_sec = 1000 * 1000 * 1000
                ms_to_sec = 1000 * 1000

                fig = data_set.compaction_df[plot_features].plot(subplots=True)
                mkdir_p(output_path)

                plt.savefig(
                    output_path + log_dir.replace(log_dir_prefix, "").replace("StorageMaterial.", "").replace("/",
                                                                                                              "_") + ".png",
                    bbox_inches="tight")
                plt.cla()
                plt.close("all")

                # read from data_set to get the compaction/flush data
                io_stats_sum_row = [(sum(data_set.compaction_df[x]) + sum(data_set.flush_df[x])) / ns_to_sec for x in
                                    io_stats_feature]
                # read from stdout file to get the thread waiting data
                mutex_waiting = std_reporter.tradeoff_data["rocksdb.db.mutex.wait.micros"]
                io_stats_sum_row.append(mutex_waiting / ns_to_sec)
                thread_pool_waiting_map = std_reporter.read_the_compaction_waiting_time()
                compaction_thread_waiting_time = 0.0
                flush_thread_waiting_time = 0.0
                for thread_waiting in thread_pool_waiting_map:
                    if "compaction" in thread_waiting:
                        compaction_thread_waiting_time += sum(thread_pool_waiting_map[thread_waiting])
                    if "flush" in thread_waiting:
                        flush_thread_waiting_time = sum(thread_pool_waiting_map[thread_waiting])

                io_stats_sum_row.append(compaction_thread_waiting_time / ms_to_sec)
                io_stats_sum_row.append(flush_thread_waiting_time / ms_to_sec)

                io_stats_sum_row_prettify = [round(x, 2) for x in io_stats_sum_row]

                splitted_log_path = log_dir.replace("StorageMaterial.", "").replace("CPU", "").split("/")
                header_list = splitted_log_path[-3:]
                prettify_header_list = splitted_log_path[-3:]

                header_list.extend(io_stats_sum_row)
                prettify_header_list.extend(io_stats_sum_row_prettify)

                result_list.append(header_list)
                prettify_result_list.append(prettify_header_list)

    header_features = ["material", "thread", "batch_size", "file_write (sec)", "file_range_sync (sec)",
                       "file_fsync (sec)", "file_prepare_write (sec)", "mutex_waiting (sec)",
                       "compaction_task_waiting (sec)", "flush_task_waiting (sec)"]

    pd.DataFrame(result_list, columns=header_features).to_csv(output_path + "raw_data.csv", index=False)
    pd.DataFrame(prettify_result_list, columns=header_features).to_csv(output_path + "pretty_raw_data.csv", index=False)
