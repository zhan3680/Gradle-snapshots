import pandas as pd
import scipy.stats as stats
import datetime
import numpy as np
import statistics
import matplotlib.pyplot as plt

if __name__ == '__main__':
    # bigtop, bookkeeper, calcite, geode, groovy, jmeter, kafka, lucene, ofbiz, poi, samza, solr, tapestry, xmlbeans

    # RQ1: relation between project age and project compilability
    # project_age = [2946, 346, 832, 2498, 4884, 941, 2923, 819, 2065, 1894, 3122, 819, 4037, 63]
    # project_size = [1887, 229, 1134, 10236, 14272, 1302, 9009, 3130, 5395, 3873, 2457, 3092, 3581, 96]
    # project_compilability = [0.86, 0.09, 1, 0.33, 0.18, 1, 0.88, 0.66, 0.53, 0.29, 0.63, 0.41, 0.49, 0.92]
    # plt.scatter(project_size, project_compilability)
    # plt.xlabel("Change History Length (#commits)")
    # plt.ylabel("Compilability")
    # plt.show()
    #
    # project_size = stats.rankdata(project_size, method='min')
    # project_compilability = stats.rankdata(project_compilability, method='min')
    # correlation, kendall_pvalue = stats.kendalltau(project_size, project_compilability, alternative='less')
    # print(correlation)
    # print(kendall_pvalue)




    #RQ2: relation between snapshot age and snsapshot compilability
    bigtop = pd.read_csv("bigtop.csv", parse_dates=['date'])
    bigtop['date'] = bigtop['date'].apply(lambda x: (x-pd.Timestamp("2014-02-04")).days)

    bookkeeper = pd.read_csv("bookkeeper.csv", parse_dates=['date'])
    bookkeeper['date'] = bookkeeper['date'].apply(lambda x: (x - pd.Timestamp("2021-03-19")).days)

    calcite = pd.read_csv("calcite.csv", parse_dates=['date'])
    calcite['date'] = calcite['date'].apply(lambda x: (x - pd.Timestamp("2019-11-19")).days)

    geode = pd.read_csv("geode.csv", parse_dates=['date'])
    geode['date'] = geode['date'].apply(lambda x: (x - pd.Timestamp("2015-04-28")).days)

    groovy = pd.read_csv("groovy.csv", parse_dates=['date'])
    groovy['date'] = groovy['date'].apply(lambda x: (x - pd.Timestamp("2008-10-15")).days)

    jmeter = pd.read_csv("jmeter.csv", parse_dates=['date'])
    jmeter['date'] = jmeter['date'].apply(lambda x: (x - pd.Timestamp("2019-08-02")).days)

    kafka = pd.read_csv("kafka.csv", parse_dates=['date'])
    kafka['date'] = kafka['date'].apply(lambda x: (x - pd.Timestamp("2014-02-27")).days)

    lucene = pd.read_csv("lucene.csv", parse_dates=['date'])
    lucene['date'] = lucene['date'].apply(lambda x: (x - pd.Timestamp("2019-12-02")).days)

    ofbiz = pd.read_csv("ofbiz.csv", parse_dates=['date'])
    ofbiz['date'] = ofbiz['date'].apply(lambda x: (x - pd.Timestamp("2016-07-04")).days)

    poi = pd.read_csv("poi.csv", parse_dates=['date'])
    poi['date'] = poi['date'].apply(lambda x: (x - pd.Timestamp("2016-12-22")).days)

    samza = pd.read_csv("samza.csv", parse_dates=['date'])
    samza['date'] = samza['date'].apply(lambda x: (x - pd.Timestamp("2013-08-12")).days)

    solr = pd.read_csv("solr.csv", parse_dates=['date'])
    solr['date'] = solr['date'].apply(lambda x: (x - pd.Timestamp("2019-12-02")).days)

    tapestry = pd.read_csv("tapestry.csv", parse_dates=['date'])
    tapestry['date'] = tapestry['date'].apply(lambda x: (x - pd.Timestamp("2011-02-09")).days)

    xmlbeans = pd.read_csv("xmlbeans.csv", parse_dates=['date'])
    xmlbeans['date'] = xmlbeans['date'].apply(lambda x: (x - pd.Timestamp("2021-12-27")).days)

    projects = [bigtop, bookkeeper, calcite, geode, groovy, jmeter, kafka, lucene, ofbiz, poi, samza, solr, tapestry, xmlbeans]


    # all_dates = []
    # for p in projects:
    #     all_dates.extend(p['date'].tolist())
    # # print(len(all_dates))
    # all_dates = np.array(all_dates)
    # first_quartile = np.percentile(all_dates, 25)
    # third_quartile = np.percentile(all_dates, 75)

    early = []
    intermediate = []
    recent = []
    num_early = 0
    num_early_success = 0
    num_intermediate = 0
    num_intermediate_success = 0
    num_recent = 0
    num_recent_success = 0
    for p in projects:
        all_dates = np.array(p['date'].tolist())
        first_quartile = np.percentile(all_dates, 25)
        third_quartile = np.percentile(all_dates, 75)

        recent_snapshots = p[p['date'] > third_quartile]
        recent_successful_snapshots = recent_snapshots[recent_snapshots['result'] == 'Success']
        if recent_snapshots.shape[0] != 0:
            recent.append(recent_successful_snapshots.shape[0] / recent_snapshots.shape[0])
        num_recent += recent_snapshots.shape[0]
        num_recent_success += recent_successful_snapshots.shape[0]

        intermediate_snapshots_temp = p[p['date'] >= first_quartile]
        intermediate_snapshots = intermediate_snapshots_temp[intermediate_snapshots_temp['date'] <= third_quartile]
        intermediate_successful_snapshots = intermediate_snapshots[intermediate_snapshots['result'] == 'Success']
        if intermediate_snapshots.shape[0] != 0:
            intermediate.append(intermediate_successful_snapshots.shape[0] / intermediate_snapshots.shape[0])
        num_intermediate += intermediate_snapshots.shape[0]
        num_intermediate_success += intermediate_successful_snapshots.shape[0]

        early_snapshots = p[p['date'] < first_quartile]
        early_successful_snapshots = early_snapshots[early_snapshots['result'] == 'Success']
        if early_snapshots.shape[0] != 0:
            early.append(early_successful_snapshots.shape[0] / early_snapshots.shape[0])
        num_early += early_snapshots.shape[0]
        num_early_success += early_successful_snapshots.shape[0]

    early = np.array(early)
    intermediate = np.array(intermediate)
    recent = np.array(recent)

    print("early: {:.2f}, {:.2f}, {:.2f}, {:.2f}, {:.2f}, {:.2f}, {:.2f}".format(np.min(early),
                                                     np.percentile(early, 25),
                                                     np.percentile(early, 50),
                                                     np.mean(early),
                                                     np.percentile(early, 75),
                                                     np.max(early),
                                                     np.std(early)))
    print("intermediate: {:.2f}, {:.2f}, {:.2f}, {:.2f}, {:.2f}, {:.2f}, {:.2f}".format(np.min(intermediate),
                                                     np.percentile(intermediate, 25),
                                                     np.percentile(intermediate, 50),
                                                     np.mean(intermediate),
                                                     np.percentile(intermediate, 75),
                                                     np.max(intermediate),
                                                     np.std(intermediate)))
    print("recent: {:.2f}, {:.2f}, {:.2f}, {:.2f}, {:.2f}, {:.2f}, {:.2f}".format(np.min(recent),
                                                     np.percentile(recent, 25),
                                                     np.percentile(recent, 50),
                                                     np.mean(recent),
                                                     np.percentile(recent, 75),
                                                     np.max(recent),
                                                     np.std(recent)))

    KWH_statistic, KW_pvalue = stats.kruskal(early, intermediate, recent)
    print(KW_pvalue)


