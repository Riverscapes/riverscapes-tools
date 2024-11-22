# Name:     Plotting
#
# Purpose:  Support methods for generating various MatPlotLib charts.
#           The main use is for validation.
#
# Author:   Philip Bailey
#
# Date:     15 Aug 2019
# -------------------------------------------------------------------------------
import matplotlib.pyplot as plt
from scipy import stats
import os


def validation_chart(values, chart_title):

    if len(values) < 1:
        raise Exception('No data values to chart.')

    file_path = os.path.join('docs/assets/images/validation', chart_title.replace(' ', '_').lower() + '.png')
    xyscatter(values, 'BRAT 3', 'BRAT 4', chart_title, file_path, True)


def xyscatter(values, xlabel, ylabel, chart_title, file_path, one2one=False):
    """
    Generate an XY scatter plot
    :param values: List of tuples containing the pairs of X and Y values
    :param xlabel: X Axis label
    :param ylabel: Y Axis label
    :param chart_title: Chart title (code appends sample size)
    :param file_path: RELATIVE file path where the figure will be saved
    :return: none
    """

    x = [x for x, y in values]
    y = [y for x, y in values]

    plt.clf()
    plt.scatter(x, y, c='#004793', alpha=0.5, label='{} (n = {:,})'.format(chart_title, len(x)))
    plt.title = chart_title
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    # TODO: add timestamp text element to the chart (to help when reviewing validation charts)

    if one2one:
        if len(x) < 3:
            raise Exception('Attempting linear regression with less than three data points.')

        m, c, r_value, _p_value, _std_err = stats.linregress(x, y)

        min_value = min(x)  # min(min(x), min(y))
        max_value = max(x)  # max(max(x), max(y))
        plt.plot([min_value, max_value], [m * min_value + c, m * max_value + c], 'blue', lw=1, label='regression m: {:.2f}, r2: {:.2f}'.format(m, r_value))
        plt.plot([min_value, max_value], [min_value, max_value], 'red', lw=1, label='1:1')

    plt.legend(loc='upper left')
    plt.tight_layout()

    if not os.path.isdir(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    plt.savefig(file_path)


def box_plot(values, chart_title, file_path):
    clean_values = [0 if x is None else x for x in values]

    plt.clf()

    plt.boxplot(clean_values, vert=False, meanline=True)
    # plt.violisnplot(clean_values, showmeans=False, showmedians=True)
    plt.title(chart_title)

    if not os.path.isdir(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    plt.savefig(file_path)


def histogram(values, bins, file_path, xlabel=None, ylabel=None, title=None):

    plt.clf()
    plt.hist(values, bins)

    if title is not None:
        plt.title(title)

    if xlabel is not None:
        plt.xlabel(xlabel)
    if ylabel is not None:
        plt.ylabel(ylabel)

    if not os.path.isdir(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    plt.savefig(file_path)


def line(x_values, y_values, xlabel, ylabel, chart_title, file_path):

    plt.clf()
    plt.plot(x_values, y_values)

    plt.title = chart_title
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    if not os.path.isdir(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    plt.savefig(file_path)


def get_overlapping_groups(labels):
    """Group overlapping labels, for pie charts."""

    groups = []

    # group labels by the distance between their centers
    for label in labels:
        for group in groups:
            if (
                abs(label.get_position()[1] - group[0].get_position()[1]) < 0.1
                and abs(label.get_position()[0] - group[0].get_position()[0]) < 1
            ):
                group.append(label)
                break
        else:
            groups.append([label])

    return groups


def group_pie_labels(labels):
    """Improve formatting of overlapping labels in pie charts."""

    groups = get_overlapping_groups(labels)

    # if more than one label in group, combine text and hide others
    for group in groups:
        if len(group) > 1:
            # reversed order is more likely to be correct, if the group is on right side
            group[0].set_text(
                ",\n".join(label.get_text() for label in reversed(group))
            )
            for label in group[1:]:
                label.set_visible(False)

    return labels


def pie(x_values, labels, chart_title, colors, file_path):

    clean_labels = []
    clean_values = []

    for i, x in enumerate(x_values):
        if x:
            clean_labels.append(labels[i])
            clean_values.append(x)

    plt.clf()
    fig, ax = plt.subplots()
    chart = ax.pie(clean_values, labels=clean_labels, colors=colors, autopct='%1.0f%%')

    group_pie_labels(chart[1])

    ax.set_title(chart_title)

    if not os.path.isdir(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    plt.tight_layout()
    plt.savefig(file_path, bbox_inches="tight")
    plt.close()


def horizontal_bar(x_values, labels, color, x_axis_label1, chart_title, file_path, x_axis_label2=None):

    clean_values = [0 if x is None else x for x in x_values]
    bar_labels = [(i / sum(clean_values)) * 100 for i in clean_values]

    plt.clf()
    fig, ax = plt.subplots()

    hbars = ax.barh(labels, clean_values, color=color)
    ax.bar_label(hbars, labels=['%.0f %%' % i for i in bar_labels], padding=5)
    ax.set_xlim((0, max(clean_values) + 0.15 * max(clean_values)))
    ax.set_xlabel(x_axis_label1)

    plt.grid(True, which='major', axis='x')

    if x_axis_label2 is not None:
        def km2mi(x):
            return x * 0.621371

        def mi2km(x):
            return x * 1.60934

        secax = ax.secondary_xaxis('top', functions=(km2mi, mi2km))
        secax.set_xlabel(x_axis_label2)

    ax.set_title(chart_title)

    if not os.path.isdir(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    plt.tight_layout()
    plt.savefig(file_path)
    plt.close()


def vertical_bar(values, labels, y_axis_label, chart_title, file_path, color="#004793"):
    """Make a vertical bar chart. The values and labels must be the same length.

    Args:
        values (list[float|int]): The values to plot.
        labels (list[str]): The labels for each value.
        y_axis_label (str): The label for the y-axis of the chart.
        chart_title (str): The title of the chart.
        file_path (str): The file path where the chart will be saved.
        color (str): The color of the bars in the chart.

    Returns:
        None
    """

    clean_values = [0 if x is None else x for x in values]

    plt.clf()

    plt.bar(labels, clean_values, color=color)
    plt.xticks(rotation=45, ha='right')

    plt.ylabel(y_axis_label)
    plt.xlabel('')
    plt.title(chart_title)

    plt.grid(True, which='major', axis='y')

    if not os.path.isdir(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    # plt.tight_layout()
    plt.savefig(file_path, bbox_inches="tight")
