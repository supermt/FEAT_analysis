import plotly.express as px
from utils.traversal import get_log_dirs, get_log_and_std_files
from utils.stdoutreader import StdoutReader
import pandas as pd

def stdout_to_dict(stdout_recorder):
    temp_dict = {}
    temp_dict["fillrandom_speed"] = stdout_recorder.benchmark_results["fillrandom"][1].split(" ")[0]
    temp_dict["threads"] = int(stdout_recorder.cpu_count.replace("CPU", ""))
    temp_dict["batch_size"] = stdout_recorder.batch_size
    temp_dict["device"] = stdout_recorder.device

    return temp_dict


if __name__ == '__main__':

    log_dir_prefix = "../LOG_DIR/PM_results_traversal_1hour/"
    dirs = get_log_dirs(log_dir_prefix)

    metrics_in_std_files = []
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
        metrics_in_std_files.append(StdoutReader(stdout_file))

    thread_list = [x for x in range(20)]
    paint_df = [stdout_to_dict(x).values() for x in metrics_in_std_files]
    paint_df = pd.DataFrame(paint_df, columns=["IOPS", "threads", "batch_size", "device"])
    paint_df["IOPS"] = paint_df["IOPS"].astype(int)
    paint_df["threads"] = paint_df["threads"].astype(int)
    # paint_df = paint_df.sort_values(by=["device", "threads"])
    # paint_df = paint_df.reindex()
    paint_df.to_csv("csv_results/traversal_overview.csv")
    print(paint_df)