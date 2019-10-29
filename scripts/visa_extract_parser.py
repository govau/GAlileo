DATA_DIR = '../data/'

import pandas as pd
import networkx as nx
from unidecode import unidecode

# https://stackoverflow.com/a/7734686
import sys

if sys.version_info.major == 3:
    from urllib.parse import urlencode, urlparse, urlunparse, parse_qs
else:
    from urllib import urlencode
    from urlparse import urlparse, urlunparse, parse_qs


def clean_url_queries(url):
    # https://stackoverflow.com/a/7734686
    if '?' not in url:
        return url
    u = urlparse(url)
    query = parse_qs(u.query, keep_blank_values=True)
    for q in ['page', 'fbclid', 'from', 'isappinstalled', 'sfns', '_e_pi_', 'from_source', 'ifm', 'mt']:
        query.pop(q, None)
    u = u._replace(query=urlencode(query, True))
    return urlunparse(u)


G = nx.DiGraph()

chunksize = 10000  # http://acepor.github.io/2017/08/03/using-chunksize/
raw_sequences = {}
raw_from_to = {}
sequence_hashes = {}
sequence_counts = {}

for f in ['data_visa_extract_ausgov.csv']:  # , 'data_visa_extract_immi.csv']:
    lastSessionId = None
    # read CSV in chunks to reduce memory overload
    for chunk in pd.read_csv(DATA_DIR + f, chunksize=chunksize):
        for index, row in chunk.iterrows():
            sessionId = row['fullVisitorId'] + row['visitId']
            if sessionId not in raw_sequences:
                raw_sequences[sessionId] = []
            # look at URLs from page hit paths and outbound link click events
            for target in [row['pagePath'], row['outboundLinkURL']]:
                # ignore blank columns and google translate URLs
                if not pd.isnull(target) and not target.startswith('/translate'):
                    target = unidecode(target)
                    # normalise paths to start with australia.gov.au
                    if target[0] == '/':
                        target = 'https://www.australia.gov.au' + target
                    target = clean_url_queries(target)
                    # remove duplicate hits eg. page then click on same URL
                    if target not in raw_sequences[sessionId]:
                        # if there is a session history, start building from/to edges
                        if len(raw_sequences[sessionId]) > 0:
                            previousAusGovPage = raw_sequences[sessionId][-1]
                            # click events should come back to ausgov on from/to network
                            if row['hitType'] == 'EVENT':
                                for seq in reversed(raw_sequences[sessionId]):
                                    if 'australia.gov.au' in seq:
                                        previousAusGovPage = seq
                            if previousAusGovPage not in raw_from_to:
                                raw_from_to[previousAusGovPage] = {}
                            # increment count for edge weight
                            raw_from_to[previousAusGovPage][target] = \
                                raw_from_to[previousAusGovPage].get(target, 0) + 1
                        # add hit URL to session sequence of URLs
                        raw_sequences[sessionId].append(target)
            # start a new session data object when the session id changes
            if lastSessionId != sessionId:
                sequenceHash = hash(str(raw_sequences[sessionId]))
                if sequenceHash not in sequence_hashes:
                    sequence_hashes[sequenceHash] = raw_sequences[sessionId]
                sequence_counts[sequenceHash] = sequence_counts.get(sequenceHash, 0) + 1
            # print progress of CSV parsing
            if index % 10000 == 0:
                print(f, index)
            lastSessionId = sessionId
    print(f, 'done')
    # write session journeys to CSV
    with open('visa_extract_ausgov_journeys.csv', 'w') as csvfile:
        csvfile.write("%s,%s\n" % ('count', 'journey_path'))
        for shash, scount in sequence_counts.items():
            # write to CSV journeys that have happened more than once
            if scount > 1:
                csvfile.write('%s,"%s"\n' % (scount, ';'.join(sequence_hashes[shash])))
            # print frequent journeys
            if scount > 500:
                print(scount, sequence_hashes[shash])
    # write from/to edges to GEXF and CSV
    with open('visa_extract_ausgov_fromto.csv', 'w') as csvfile:
        csvfile.write("%s,%s,%s\n" % ('from_url', 'to_url', 'weight'))
        for from_url, urls in raw_from_to.items():
            for to_url in urls.keys():
                G.add_edge(from_url,
                           to_url,
                           weight=urls[to_url])
                csvfile.write("%s,%s,%s\n" % (from_url, to_url, urls[to_url]))
    nx.write_gexf(G, "visa_extract_ausgov.gexf")
