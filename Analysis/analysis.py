import itertools
from os import listdir
from os.path import isfile, join

import numpy as np
import pandas as pd
import random as rand

import scipy.stats as stats
import matplotlib.pyplot as plt

def read_files():
    mypath = "data/first_correct_run"
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    onlyfiles = sorted(onlyfiles)

    data = np.zeros((len(onlyfiles), 6))

    # algo
    # static
    # nodes
    # pipeline
    # numberof_groups
    # jobs_per_group
    # trial
    # fairness
    # utilization
    print(onlyfiles)
    for i, f in enumerate(onlyfiles):
        splitted = f.split("-")
        print(splitted)
        data[i, 0] = splitted[2]
        data[i, 1] = splitted[3]
        data[i, 2] = splitted[4]
        data[i, 3] = splitted[5]
        data[i, 4] = splitted[7]
        data[i, 5] = splitted[8].split(".csv")[0]

    return pd.DataFrame(data, columns=['nodes', 'pipeline', 'number_of_groups', 'jobs_per_group', 'fairness', 'utilization'])

def n_way_anova(df):
    labels = {}
    labels[1] = ['nodes', 'pipeline', 'number_of_groups', 'jobs_per_group']
    for i in [2, 3, 4]:
        labels[i] = list(itertools.combinations(labels[1], i))

    obs_list = ['fairness']
    for k in labels.keys():
        print(str(k) + " : " + str(labels[k]))

    # Computing main and interaction effects
    effects = {}
    effects[0] = {'x0': [df['fairness'].mean()]}
    print(effects[0])

    effects[1] = {}
    for key in labels[1]:
        effects_result = []
        for obs in obs_list:
            effects_df = df.groupby(key)[obs].mean()
            result = sum([ zz*effects_df.loc[zz] for zz in effects_df.index])
            effects_result.append(result)
        effects[1][key] = effects_result
    print(effects[1])

    for c in [2, 3, 4]:
        effects[c] = {}
        for key in labels[c]:
            effects_result = []
            for obs in obs_list:
                print(df)
                print(key)
                effects_df = df.groupby(list(key))[obs].mean()
                result = sum([np.prod(zz) * effects_df.loc[zz] / (2 ** (len(zz) - 1)) for zz in effects_df.index])
                effects_result.append(result)
            effects[c][key] = effects_result

    def printd(d):
        for k in d.keys():
            print("%25s : %s" % (k, d[k]))

    for i in range(1, 5):
        printd(effects[i])

    master_dict = {}
    for nvars in effects.keys():

        effect = effects[nvars]
        for k in effect.keys():
            v = effect[k]
            master_dict[k] = v

    master_df = pd.DataFrame(master_dict).T
    master_df.columns = obs_list

    y1 = master_df['fairness'].copy()
    y1.sort_values(inplace=True, ascending=False)

    print("Top 10 effects for observable fairness:")
    print(y1[:20])

    # Quantify which effects are not normally distributed,
    # to assist in identifying important variables

    fig = plt.figure(figsize=(14, 4))
    ax1 = fig.add_subplot(131)

    stats.probplot(y1, dist="norm", plot=ax1)
    ax1.set_title('y1')

    plt.show()


def create_pareto_plot(df):
    plt.scatter(df['fairness'], df['utilization'])
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel('fairness')
    plt.ylabel('utilization')
    plt.show()


def main():
    df = read_files()
    print(df)
    create_pareto_plot(df)
    # n_way_anova(df)
    # maov = MANOVA.from_formula('nodes + pipeline + number_of_groups + jobs_per_group '
    #                            ' + nodes * pipeline + nodes * number_of_groups + nodes * jobs_per_group '
    #                            ' + pipeline * number_of_groups + pipeline * jobs_per_group '
    #                            ' + number_of_groups * jobs_per_group '
    #                            ' + nodes * pipeline * number_of_groups + nodes * pipeline * jobs_per_group + nodes * number_of_groups * jobs_per_group '
    #                            ' + pipeline * number_of_groups * jobs_per_group '
    #                            ' + nodes * pipeline * number_of_groups * jobs_per_group '
    #                            ' ~ fairness', data=df)
    #
    # print(maov)
    # # print(maov.)
    # print(maov.mv_test())
    #
    # model = ols('fairness ~ C(nodes) + C(pipeline) + C(number_of_groups) + C(jobs_per_group) '
    #             '+ C(nodes):C(pipeline) + C(nodes):C(number_of_groups) + C(nodes):C(jobs_per_group) '
    #             '+ C(pipeline):C(number_of_groups) + C(pipeline):C(jobs_per_group) '
    #             '+ C(number_of_groups):C(jobs_per_group) '
    #             '+ C(nodes):C(pipeline):C(number_of_groups) + C(nodes):C(pipeline):C(jobs_per_group) + C(nodes):C(number_of_groups):C(jobs_per_group)'
    #             '+ C(pipeline):C(number_of_groups):C(jobs_per_group) '
    #             '+ C(nodes):C(pipeline):C(number_of_groups):C(jobs_per_group) ', data=df).fit()
    # anova = sm.stats.anova_lm(model, typ=3)
    # np.set_printoptions(threshold=np.inf)
    # pd.set_option("display.max_rows", 1000, "display.max_columns", 1000)
    # print(anova)

if __name__ == '__main__':
    main()