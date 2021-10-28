import itertools
from os import listdir
from os.path import isfile, join

import numpy as np
import pandas as pd
import random as rand

import scipy.stats as stats
import matplotlib.pyplot as plt
from test import VD_A
from scipy.stats import wilcoxon

def read_files(type):
    mypath = "data/dynamic"
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    onlyfiles = sorted(onlyfiles)

    data = dict()

    #0 algo
    #1 static
    #2 nodes
    #3 pipeline
    #4 numberof_groups
    #5 jobs_per_group
    #6 trial
    #7 fairness
    #8 utilization
    #9 norm fairness
    #10 norm utilization
    print(onlyfiles)
    for i, f in enumerate(onlyfiles):
        splitted = f.split("-")
        if splitted[0] not in data:
            data[splitted[0]] = []

        if len(data[splitted[0]]) >= 10:
            continue

        if type == 7 or type == 9:
            data[splitted[0]].append(1/float(splitted[type]))
        else:
            data[splitted[0]].append(float(splitted[type]))

        # data[splitted[0]].append(float(splitted[10].split('.csv')[0]))


    return data


def compare_two(data0, data1):

    data0 = np.array(data0)
    data1 = np.array(data1)

    if np.all(data0 == data1):
        return '-', '-'

    result = wilcoxon(data0, data1)
    vargha = VD_A([float(x) for x in data1], [float(x) for x in data0])

    return "{:.3f}".format(result[1]), f'{vargha[0]} ({vargha[1]})'


def main():
    data_util = read_files(8)
    data_fairness = read_files(9)

    random_data = data_util['random']
    fifo_data = data_util['fifo']
    fair_data = data_util['fair']

    random_data_fair = data_fairness['random']
    fifo_data_fair = data_fairness['fifo']
    fair_data_fair = data_fairness['fair']

    # p, a = compare_two(random_data, fifo_data)
    print(" & ".join(['Random', '{:.3f}'.format(np.mean(random_data_fair)), '{:.4f}'.format(np.std(random_data_fair)), '{:.3f}'.format(np.mean(random_data)), '{:.4f}'.format(np.std(random_data))]))
    print(" & ".join(['FiFo', '{:.3f}'.format(np.mean(fifo_data_fair)), '{:.4f}'.format(np.std(fifo_data_fair)), '{:.3f}'.format(np.mean(fifo_data)), '{:.4f}'.format(np.std(fifo_data))]))
    print(" & ".join(['Fair', '{:.3f}'.format(np.mean(fair_data_fair)), '{:.4f}'.format(np.std(fair_data_fair)), '{:.3f}'.format(np.mean(fair_data)), '{:.4f}'.format(np.std(fair_data))]))
    print()

    p, a = compare_two(random_data, fifo_data)
    print(" & ".join(['Random-FiFo', p, a, '{:.2f}'.format((np.mean(fifo_data) - np.mean(random_data))/np.mean(random_data))]))

    p, a = compare_two(random_data, fair_data)
    print(" & ".join(['Random-Fair', p, a, '{:.2f}'.format((np.mean(fair_data) - np.mean(random_data))/np.mean(random_data))]))

    p, a = compare_two(fifo_data, fair_data)
    print(" & ".join(['FiFo-Fair', p, a, '{:.2f}'.format((np.mean(fair_data) - np.mean(fifo_data))/np.mean(fifo_data))]))

    print()
    p, a = compare_two(random_data_fair, fifo_data_fair)
    print(" & ".join(['Random-FiFo', p, a, '{:.2f}'.format((np.mean(fifo_data_fair) - np.mean(random_data_fair))/np.mean(random_data_fair))]))

    p, a = compare_two(random_data_fair, fair_data_fair)
    print(" & ".join(['Random-Fair', p, a, '{:.2f}'.format((np.mean(fair_data_fair) - np.mean(random_data_fair))/np.mean(random_data_fair))]))

    p, a = compare_two(fifo_data_fair, fair_data_fair)
    print(" & ".join(['FiFo-Fair', p, a, '{:.2f}'.format((np.mean(fair_data_fair) - np.mean(fifo_data_fair))/np.mean(fifo_data_fair))]))

if __name__ == '__main__':
    main()