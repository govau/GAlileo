DATA_DIR = '../data/'

import pandas as pd
import networkx as nx
from unidecode import unidecode
G = nx.DiGraph()

chunksize = 10000 # http://acepor.github.io/2017/08/03/using-chunksize/
raw_sequences = {}
raw_from_to = {}
sequence_hashes = {}
sequence_counts = {}

for f in ['data_visa_extract_ausgov.csv']:  # , 'data_visa_extract_immi.csv']:
    lastSessionId = None
    for chunk in pd.read_csv(DATA_DIR + f, chunksize=chunksize):
        for index, row in chunk.iterrows():
            sessionId = row['fullVisitorId'] + row['visitId']
            if sessionId not in raw_sequences:
                raw_sequences[sessionId] = []

            for target in [row['pagePath'], row['outboundLinkURL']]:
                if not pd.isnull(target):
                    target = unidecode(target)
                    if target[0] == '/':
                        target = 'https://www.australia.gov.au' + target
                    if target not in raw_sequences[sessionId]:
                        if len(raw_sequences[sessionId]) > 0:
                            previousAusGovPage = raw_sequences[sessionId][-1]
                            # click events should come back to ausgov on from/to network, not from immi to immi
                            # so look back 2 steps in sequence
                            if 'australia.gov.au' not in previousAusGovPage:
                                previousAusGovPage = raw_sequences[sessionId][-2]
                            if previousAusGovPage not in raw_from_to:
                                raw_from_to[previousAusGovPage] = {}
                            raw_from_to[previousAusGovPage][target] = \
                                raw_from_to[previousAusGovPage].get(target, 0) + 1
                        raw_sequences[sessionId].append(target)
            if lastSessionId != sessionId:
                sequenceHash = hash(str(raw_sequences[sessionId]))
                if sequenceHash not in sequence_hashes:
                    sequence_hashes[sequenceHash] = raw_sequences[sessionId]
                sequence_counts[sequenceHash] = sequence_counts.get(sequenceHash, 0) + 1
            if index % 10000 == 0:
                print(f, index)
            lastSessionId = sessionId
    print(f, 'done')
    with open('visa_extract_ausgov_journeys.csv', 'w') as csvfile:
        csvfile.write("%s,%s\n" % ('count', 'journey_path'))
        for shash, scount in sequence_counts.items():
            if scount > 1:
                csvfile.write('%s,"%s"\n' % (scount, ';'.join(sequence_hashes[shash])))
            if scount > 500:
                print(scount, sequence_hashes[shash])
    G.add_nodes_from(raw_from_to.keys())
    with open('visa_extract_ausgov_fromto.csv', 'w') as csvfile:
        csvfile.write("%s,%s,%s\n" % ('from_url', 'to_url', 'weight'))
        for from_url, urls in raw_from_to.items():
            for to_url in urls.keys():
                if urls[to_url] > 1:
                    G.add_edge(from_url.encode('ascii',errors='ignore'),
                               to_url.encode('ascii',errors='ignore'),
                               weight=urls[to_url])
                    csvfile.write("%s,%s,%s\n" % (from_url, to_url, urls[to_url]))
    nx.write_gexf(G, "visa_extract_ausgov.gexf")
