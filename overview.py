import plotly.express as px
from utils.traversal import get_log_dirs, get_log_and_std_files
from utils.stdoutreader import StdoutReader
import pandas as pd


def stdout_to_dict(stdout_recorder):
    temp_dict = {}
    temp_dict["fillrandom_speed"] = stdout_recorder.benchmark_results["fillrandom"][1].split(" ")[0]
    temp_dict["threads"] = stdout_recorder.cpu_count.replace("CPU", " threads")
    temp_dict["batch_size"] = stdout_recorder.batch_size
    temp_dict["device"] = stdout_recorder.device

    return temp_dict


if __name__ == '__main__':

    log_dir_prefix = "LOG_DIR/pm_server_fillrandom_3600/"
    dirs = get_log_dirs(log_dir_prefix)

    metrics_in_std_files = []
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
        metrics_in_std_files.append(StdoutReader(stdout_file))

    thread_list = [1, 4, 12]
    paint_df = [stdout_to_dict(x).values() for x in metrics_in_std_files]
    paint_df = pd.DataFrame(paint_df, columns=["IOPS", "threads", "batch_size", "device"])
    paint_df["IOPS"] = paint_df["IOPS"].astype(int)
    print(paint_df)
    fig = px.bar(paint_df, x="threads", y="IOPS", color="batch_size", barmode="group", facet_col="device",
                 category_orders={"threads": [str(x) + " threads" for x in thread_list],
                                  "device": ["PM", "NVMeSSD", "SATASSD", "SATAHDD"],
                                  "batch_size":["64MB","128MB","256MB"],
                                  },
                 labels={"threads": ""}
                 )

    # fig.write_image("paper_usage/increasing_threads.pdf")
    fig.write_image("paper_usage/performance_overview.png")