import plotly.express as px
from utils.traversal import get_log_dirs, get_log_and_std_files
from utils.stdoutreader import StdoutReader
import pandas as pd
from choosing_knob_thread_impact_on_write_stall import prettify_the_fig

STALL_REASON = ["level0", "pending_compaction_bytes", "memtable"]


def aggreate_stall_type(stall_dict):
    results = {x: 0 for x in STALL_REASON}
    for key in stall_dict:
        for stall_reason in STALL_REASON:
            if stall_reason in key:
                results[stall_reason] += int(stall_dict[key])
    return results


def stdout_to_dict(stdout_recorder):
    temp_dict = {}
    temp_dict["throughput"] = stdout_recorder.benchmark_results["fillrandom"][1].split(" ")[0]
    temp_dict["threads"] = int(stdout_recorder.cpu_count.replace("CPU", ""))
    temp_dict["batch_size"] = stdout_recorder.batch_size.replace("MB", "")
    temp_dict["device"] = stdout_recorder.device

    temp_dict.update(aggreate_stall_type(stdout_recorder.stall_reasons))

    return temp_dict


if __name__ == '__main__':

    log_dir_prefix = "LOG_DIR/PM_server_results_400M_write_40M_read/"
    dirs = get_log_dirs(log_dir_prefix)

    metrics_in_std_files = []
    temp = {}
    for log_dir in dirs:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
        metrics_in_std_files.append(StdoutReader(stdout_file))

    thread_list = [x for x in range(20)]
    paint_df = [stdout_to_dict(x).values() for x in metrics_in_std_files]
    columns = stdout_to_dict(metrics_in_std_files[0]).keys()
    paint_df = pd.DataFrame(paint_df, columns=columns)
    paint_df["throughput"] = paint_df["throughput"].astype(int)
    paint_df["threads"] = paint_df["threads"].astype(int)
    paint_df = paint_df.sort_values(by=["device", "threads"])
    paint_df = paint_df.reindex()

    for stall_reason in STALL_REASON:
        paint_df[stall_reason]


