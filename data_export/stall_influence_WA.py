import csv
import os.path

import pandas as pd
import plotly.express as px
from pandas import read_csv

from utils.stdoutreader import StdoutReader
from utils.traversal import mkdir_p, get_log_dirs, get_log_and_std_files


def normalize(df_array):
    max_value = df_array.max()
    min_value = df_array.min()
    result = (df_array - min_value) / (max_value - min_value) + 1
    return result


if __name__ == '__main__':
    log_dir_prefix = "../LOG_DIR/PM_server_quicksand_with_read/"

    dirs = get_log_dirs(log_dir_prefix)

    metrics_in_std_files = []
    for log_dir in dirs:
        # if "1CPU" in log_dir:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
        temp = StdoutReader(stdout_file)
        temp.stall_type = log_dir.split(os.path.sep)[-4]
        metrics_in_std_files.append(temp)

    print(len(metrics_in_std_files))
    result_dict_list = []
    filed_names = []
    for std_record in metrics_in_std_files:
        temp_dict = {}
        temp_dict["THD"] = int(std_record.cpu_count.replace("CPU", ""))
        temp_dict["material"] = std_record.device
        temp_dict["stall_type"] = std_record.stall_type
        temp_dict.update(std_record.tradeoff_data)
        temp_dict["read_performance"] = int(std_record.benchmark_results["readrandom"][1].split(" ")[0])
        temp_dict["max_pending"] = std_record.stall_influence_data["max pending bytes"]
        temp_dict["stop_pending"] = std_record.stall_influence_data["stops with pending bytes"]
        temp_dict["db_size"] = std_record.stall_influence_data["stops with Total SST file size"]

        for level in std_record.read_latency_map:
            for level_entry in std_record.read_latency_map[level]:
                temp_dict["read_latency_" + level + "_" + level_entry] = std_record.read_latency_map[level][
                    level_entry]

        result_dict_list.append(temp_dict)

    filed_names = temp_dict.keys()
    # print(filed_names)

    full_data_csv_name = "csv_results/stall_influence/full_tradeoff_data.csv"
    wa_csv_name = "csv_results/stall_influence/wa_changing.csv"
    read_latency_csv = "csv_results/stall_influence/stall_influence.csv"
    try:
        with open(full_data_csv_name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=filed_names)
            writer.writeheader()
            for data_line in result_dict_list:
                writer.writerow(data_line)
    except IOError:
        print("I/O error")
    full_data_pd = pd.read_csv(full_data_csv_name)

    wa_data = full_data_pd[["THD", "material", "stall_type"]]
    wa_data.loc[:,"wa"] = (full_data_pd["Cumulative compaction write size"] / full_data_pd["Flush(GB)"])
    wa_data = wa_data.sort_values(
        by=["stall_type", "material", "THD"],
        ignore_index=False)
    wa_data.to_csv(wa_csv_name)

    read_latency_data = full_data_pd[["THD", "material", "stall_type", "max_pending", "stop_pending", "db_size"]]
    read_latency_data.loc[:,"read_qps"] = full_data_pd["read_performance"]
    read_latency_data.to_csv(read_latency_csv)
