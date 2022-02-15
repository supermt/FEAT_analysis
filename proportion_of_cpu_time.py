# here is an example from online-ml/river

from log_class import LogRecorder
from utils.traversal import get_log_and_std_files
from utils.traversal import get_log_dirs


def load_log_and_qps(log_file, ground_truth_csv):
    # load the data
    return LogRecorder(log_file, ground_truth_csv)


def data_cleaning_by_max_MBPS(bucket_df, MAX_READ=2000, MAX_WRITE=1500):
    read = bucket_df["read"]
    bad_read = read >= MAX_READ
    read[bad_read] = MAX_READ
    write = bucket_df["write"]
    bad_write = write >= MAX_WRITE
    write[bad_write] = MAX_WRITE
    return bucket_df


def plot_lsm(lsm_shape, plot_level, fig, axes):
    axes[0, 1].set_title("Compaction Count")
    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'w']
    for i in range(plot_level):
        axes[i, 0].plot(lsm_shape["time_micro"], lsm_shape["level" + str(i)], c=colors[i])
        axes[i, 0].set_ylabel("level" + str(i))

    return axes


def plot_compaction(compaction_df, plot_level, fig, axes):
    axes[0, 0].set_title("Level File Count")
    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'w']
    for i in range(plot_level):
        axes[i, 1].plot(compaction_df["level" + str(i)], c=colors[i])
        # axes[i, 0].set_ylabel("level" + str(i))

    return axes


if __name__ == '__main__':
    log_dir_prefix = "LOG_DIR/fillrandom_baseline_20GB/"
    dirs = get_log_dirs(log_dir_prefix)
    for log_dir in dirs:
        header = log_dir.split("/")[-3:]
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
        data_set = load_log_and_qps(LOG_file, report_csv)
        total_cpu_time = data_set.compaction_df["compaction_time_cpu_micros"]
        total_compaction_time = data_set.compaction_df["compaction_time_micros"]
        ratio = total_cpu_time/total_compaction_time
        ratio = ratio.mean()
        print("%s,%s,%s,%s"%(header[0].replace("StorageMaterial.",""), header[1].replace("CPU",""),header[2],round((ratio*100),2)))