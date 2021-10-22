import pandas as pd
import numpy as np
from numpy.random import rand, seed
import itertools


def n_way_anova():
    # Create the inputs:
    encoded_inputs = list(itertools.product([-1, 1], [-1, 1], [-1, 1], [-1, 1], [-1, 1], [-1, 1]))

    # Create the experiment design table:
    doe = pd.DataFrame(encoded_inputs, columns=['x%d' % (i + 1) for i in range(6)])

    # "Manufacture" observed data y
    doe['y1'] = doe.apply(lambda z: sum([rand() * z["x%d" % (i)] + 0.01 * (0.5 - rand()) for i in range(1, 7)]), axis=1)
    doe['y2'] = doe.apply(lambda z: sum([5 * rand() * z["x%d" % (i)] + 0.01 * (0.5 - rand()) for i in range(1, 7)]),
                          axis=1)
    doe['y3'] = doe.apply(lambda z: sum([100 * rand() * z["x%d" % (i)] + 0.01 * (0.5 - rand()) for i in range(1, 7)]),
                          axis=1)
    print(doe[['y1', 'y2', 'y3']])

    labels = {}
    labels[1] = ['x1','x2','x3','x4','x5','x6']
    for i in [2,3,4,5,6]:
        labels[i] = list(itertools.combinations(labels[1], i))

    obs_list = ['y1','y2','y3']

    for k in labels.keys():
        print(str(k) + " : " + str(labels[k]))

    effects = {}

    # Start with the constant effect: this is $\overline{y}$
    effects[0] = {'x0': [doe['y1'].mean(), doe['y2'].mean(), doe['y3'].mean()]}
    print(effects[0])

    effects[1] = {}
    for key in labels[1]:
        effects_result = []
        for obs in obs_list:
            effects_df = doe.groupby(key)[obs].mean()
            result = sum([zz * effects_df.loc[zz] for zz in effects_df.index])
            effects_result.append(result)
        effects[1][key] = effects_result

    print(effects[1])

    for c in [2, 3, 4, 5, 6]:
        effects[c] = {}
        for key in labels[c]:
            effects_result = []
            for obs in obs_list:
                effects_df = doe.groupby(list(key))[obs].mean()
                result = sum([np.prod(zz) * effects_df.loc[zz] / (2 ** (len(zz) - 1)) for zz in effects_df.index])
                effects_result.append(result)
            effects[c][key] = effects_result

    def printd(d):
        for k in d.keys():
            print("%25s : %s" % (k, d[k]))

    for i in range(1, 7):
        printd(effects[i])


def main():
    n_way_anova()

if __name__ == '__main__':
    main()