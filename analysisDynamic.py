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

def read_files():
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
        # data[splitted[0]].append(float(splitted[8]))
        data[splitted[0]].append(float(splitted[10].split('.csv')[0]))


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
    data = read_files()

    for key0, data0 in data.items():
        for key1, data1 in data.items():
            if key0 == key1:
                continue


    random_data = data['random']
    fifo_data = data['fifo']
    fair_data = data['fair']

    # p, a = compare_two(random_data, fifo_data)
    print(" & ".join(['random', '{:.3f}'.format(np.mean(random_data)), '{:.4f}'.format(np.std(random_data)), '-', '-']))

    p, a = compare_two(random_data, fifo_data)
    print(" & ".join(['fifo', '{:.3f}'.format(np.mean(fifo_data)), '{:.4f}'.format(np.std(fifo_data)), p, a, '{:.2f}'.format((np.mean(fifo_data) - np.mean(random_data))/np.mean(random_data))]))

    p, a = compare_two(random_data, fair_data)
    print(" & ".join(['fair', '{:.3f}'.format(np.mean(fair_data)), '{:.4f}'.format(np.std(fair_data)), p, a, '{:.2f}'.format((np.mean(fair_data) - np.mean(random_data))/np.mean(random_data))]))

    p, a = compare_two(fifo_data, fair_data)
    print(" & ".join(['fair', '{:.3f}'.format(np.mean(fair_data)), '{:.4f}'.format(np.std(fair_data)), p, a, '{:.2f}'.format((np.mean(fair_data) - np.mean(fifo_data))/np.mean(fifo_data))]))

if __name__ == '__main__':
    main()