import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objs as go

from feature_selection import generate_lsm_shape
from lsm_shape import load_log_and_qps
from utils.stdoutreader import StdoutReader
from utils.traversal import get_log_dirs, get_log_and_std_files, mkdir_p


def read_from_false_csv(csv_name):
    read_results = []
    file_content = open(csv_name).readlines()
    for line in file_content[1:]:
        print(line)
        read_results.append(int(line.split(",")[2]))
    return pd.DataFrame(read_results, columns=["secs_elapsed"])


def prettify_the_fig(fig, fontsize):
    fig.update_layout(legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.1,
        xanchor="left",
        x=0.25,
        font=dict(size=fontsize + 4)))
    fig.update_layout(font=dict(size=fontsize), autosize=False,
                      width=1400,
                      height=800,
                      margin=go.layout.Margin(
                          l=0,  # left margin
                          r=10,  # right margin
                          b=0,  # bottom margin
                          t=0  # top margin
                      )
                      )


def prettify_the_fig_with_title(fig, fontsize, title_text, x_left_shift=-0.05):
    fig.update_layout(legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.05,
        xanchor="center",
        x=0.7,
        font=dict(size=fontsize)))

    fig.add_annotation(text=title_text, x=x_left_shift, y=0.5, xref="paper", yref="paper", textangle=-90,
                       font=dict(size=fontsize + 8))
    fig.update_layout(font=dict(size=fontsize), autosize=False,
                      width=1400,
                      height=800,
                      margin=go.layout.Margin(
                          l=0,  # left margin
                          r=10,  # right margin
                          b=0,  # bottom margin
                          t=0  # top margin
                      ),
                      # title_x=0.5, title_y=0.95, title_font_size=fontsize + 8
                      )


if __name__ == '__main__':
    log_dir_prefix = "./LOGs/8.28_PC_server/"
    dirs = get_log_dirs(log_dir_prefix)

    qps_curves = {}

    for log_dir in dirs:
        # if "1CPU" in log_dir:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
        std_results = StdoutReader(stdout_file)
        qps_curves[std_results.device] = pd.read_csv(report_csv)
    plot_features = ["interval_qps"]
    for device in qps_curves:
        fig = qps_curves[device][plot_features].plot(subplots=True, ylim=(0, 60000))
        plt.tight_layout()
        file_name = "qps_curve_" + device + ".png"
        print(file_name)
        plt.savefig(file_name)

    log_dir_prefix = "./LOGs/8.28_PC_server/origin"
    dirs = get_log_dirs(log_dir_prefix)

    qps_curves = {}

    for log_dir in dirs:
        # if "1CPU" in log_dir:
        stdout_file, LOG_file, report_csv = get_log_and_std_files(log_dir)
        std_results = StdoutReader(stdout_file)
        qps_curves[
            std_results.device + "_" + std_results.cpu_count + "_" + std_results.batch_size] = read_from_false_csv(
            report_csv)
    plot_features = ["secs_elapsed"]

    for feature in qps_curves:
        fig = qps_curves[feature][plot_features].plot(ylim=(0, 60000))
        plt.tight_layout()
        file_name = "qps_curve_" + feature + ".png"
        print(file_name)
        plt.savefig(file_name)
