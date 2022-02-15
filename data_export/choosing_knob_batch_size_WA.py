import csv

import pandas as pd
import plotly.express as px

from utils.stdoutreader import StdoutReader
from utils.traversal import mkdir_p, get_log_dirs, get_log_and_std_files


def normalize(df_array):
    max_value = df_array.max()
    min_value = df_array.min()
    result = (df_array - min_value) / (max_value - min_value) + 1
    return result


if __name__ == '__main__':
    log_dir_prefix = "../LOG_DIR/PM_server_hist_100GB_write_100M_read/"

    dirs = get_log_dirs(log_dir_prefix)

    metrics_in_std_files = []
    for log_dir in dirs:
        # if "1CPU" in log_dir:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
        metrics_in_std_files.append(StdoutReader(stdout_file))

    result_dict_list = []
    filed_names = []
    for std_record in metrics_in_std_files:
        temp_dict = {}
        temp_dict["THD"] = int(std_record.cpu_count.replace("CPU", ""))
        temp_dict["material"] = std_record.device
        temp_dict["batch_size"] = std_record.batch_size
        temp_dict.update(std_record.tradeoff_data)
        for level in std_record.read_latency_map:
            for level_entry in std_record.read_latency_map[level]:
                temp_dict["read_latency_" + level + "_" + level_entry] = std_record.read_latency_map[level][
                    level_entry]

        result_dict_list.append(temp_dict)

    filed_names = temp_dict.keys()

    full_data_csv_name = "csv_results/full_tradeoff_data.csv"
    wa_csv_name = "csv_results/wa_changing.csv"
    try:
        with open(full_data_csv_name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=filed_names)
            writer.writeheader()
            for data_line in result_dict_list:
                writer.writerow(data_line)
    except IOError:
        print("I/O error")
    full_data_pd = pd.read_csv(full_data_csv_name)
    # plot_data = origin_data[["THD", "material", "batch_size", "Cumulative compaction write size"]]
    # # plot_data["Cumulative compaction write size"] = normalize(plot_data["Cumulative compaction write size"])
    #
    # cat_order = {"material": ["PM", "NVMeSSD", "SATASSD", "SATAHDD"], "batch_size": ["64MB", "128MB", "256MB", "512MB"],
    #              "THD": ["1", "4", "12", "16"]}
    #
    wa_data = full_data_pd[["THD", "material", "batch_size"]]
    wa_data["wa"] = full_data_pd["Cumulative compaction write size"] / full_data_pd["Flush(GB)"]
    wa_data = wa_data.sort_values(
        by=["batch_size", "material", "THD"],
        ignore_index=False)
    print(wa_data)
    wa_data.to_csv(wa_csv_name)
