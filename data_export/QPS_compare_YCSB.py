from utils.traversal import get_log_dirs, get_log_and_std_files
from utils.stdoutreader import StdoutReader
import pandas as pd

STALL_REASON = ["memtable", "level0", "pending_compaction_bytes"]

color_map = {"PM": "rgb(68,114,196)", "NVMeSSD": "rgb(237,125,49)", "SATASSD": "rgb(165,165,165)",
             "SATAHDD": "rgb(255,192,0)"}


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


def extract_qps(input_dir):
    input_dirs = get_log_dirs(input_dir)

    qps_dict = {}

    for log_dir in input_dirs:
        if ("2CPU" in log_dir or "1CPU" in log_dir) and "64MB" in log_dir:
            stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir, multi_tasks=True)
            basic_info = StdoutReader(stdout_file)
            qps_dict[basic_info.device] = basic_info.benchmark_results

    return qps_dict


if __name__ == '__main__':

    devices = ["PM", "NVMeSSD", "SATASSD", "SATAHDD"]
    Tuners = ["FEAT"]

    # extract_stall_and_duration()
    # extract_stall_and_duration("LOG_DIR/PM_results_DOTA_nov_5th/")
    # extract_stall_and_duration("LOG_DIR/PM_results_DOTA_nov_5th/")

    target_map = {
        "default_YCSBload60GB+zipfian_1000": extract_qps(
            "../LOG_DIR/PM_results_ycsb_load_60GB/baseline/a/"),
        "default_YCSBload60GB+latest_1000": extract_qps(
            "../LOG_DIR/PM_results_ycsb_load_60GB/baseline/d/"),
        "default_YCSBload60GB+uniform_1000": extract_qps(
            "../LOG_DIR/PM_results_ycsb_load_60GB/baseline/e/"),

        "FEAT_YCSBload60GB+zipfian_1000": extract_qps("../LOG_DIR/PM_results_ycsb_load_60GB/FEAT/a/"),
        "FEAT_YCSBload60GB+latest_1000": extract_qps("../LOG_DIR/PM_results_ycsb_load_60GB/FEAT/d/"),
        "FEAT_YCSBload60GB+uniform_1000": extract_qps("../LOG_DIR/PM_results_ycsb_load_60GB/FEAT/e/"),
    }
    result_list = []
    for i in range(len(devices)):
        for set_name in target_map:
            device = devices[i]
            tuner = set_name.split("_")[0]
            workload_dist = set_name.split("_")[1]
            value_size = set_name.split("_")[2]
            workload_duration = workload_dist.split("+")[0]
            workload_name = workload_dist.split("+")[1]
            qps = target_map[set_name][device]['ycsb_load'][1].split(" ")[0]
            row = [device, workload_duration, workload_name, value_size, tuner, qps]
            result_list.append(row)
    result_df = pd.DataFrame(result_list,
                             columns=["Device", "duration", "workload", "value size", "Tuner", "qps"])
    result_df.to_csv("csv_results/qps_compare_YCSB.csv", index=False, sep=" ")
