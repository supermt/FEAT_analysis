# here is an example from online-ml/river

import matplotlib as mpl

from log_class import LogRecorder
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


if __name__ == '__main__':
    mpl.rcParams['figure.figsize'] = (8, 6)
    mpl.rcParams['axes.grid'] = False

    log_dir_prefix = "LOG_DIR/fillrandom_log/"
    dirs = get_log_dirs(log_dir_prefix)

    for log_dir in dirs:
        if "1CPU/64MB" in log_dir:
            print(log_dir)
    #     stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
    #     data_set = load_log_and_qps(LOG_file, report_csv)
    #     bucket_df = vectorize_by_compaction_output_level(data_set,7)
    #     bucket_df = combine_vector_with_qps(bucket_df, data_set.qps_df)
    #
    #     # bucket_df = data_cleaning_by_max_MBPS(bucket_df)
    #     #
    #     fig = bucket_df.plot(subplots=True)
    #     output_path = "compaction_style/universal/%s/" % log_dir.replace(log_dir_prefix, "").replace("/", "_")
    #     mkdir_p(output_path)
    #     plt.savefig("{}/compaction_distribution_by_level.pdf".format(output_path), bbox_inches="tight")
    #     plt.savefig("{}/compaction_distribution_by_level.png".format(output_path), bbox_inches="tight")
    #     plt.close()
