# MIT Lincoln Laboratory Graph QuBE

This project contains the source for MIT-LL Graph QuBE, a tool which enables efficient pattern-of-behavior search in data containing entities transacting over time. 

Graph QuBE helps to answer the basic question: "If I have an interesting pattern of behavior between some entities-of-interest, can I look through all my data to find other such examples of this type of behavior?" Having such a "pattern of behavior" search capability could support many tasks such as situational awareness and decision support.

Developing technology for pattern-of-behavior search in large, transactional data sets, however, is a challenging task for both the commercial and academic communities. MIT-LL Graph QuBE marries the strengths of approaches from both these communties to develop an efficient two-stage system for pattern search on transactional data.

Links to the appropriate documentation are given below:

## System Description 

The following links give an overview of the underlying technology used by the Graph QuBE system. These documents provide both technical details as well as some context as to how it relates to other similar technologies. 

>[MIT-LL Graph QuBE Executive Summary] (doc/papers/MITLL_GraphQuBE_ExecutiveSummary.pdf)

>[MIT-LL Graph QuBE System Description] (doc/papers/MITLL_GraphQuBE_SystemDescription.pdf)

## Building and Running on Example Data

The following link outlines how to build, launch and use Graph QuBE (with an example data set) from the distributed source. 

>[MIT-LL Graph QuBE User Manual] (doc/XDATA_UserManual.pdf)

This manual references a provided example class that llustrates how the data ingest process can be done from the sample Bitcoin data set distributed with the source: [BitcoinIngest.java] (src/main/java/mitll/xdata/dataset/bitcoin/ingest/BitcoinIngest.java)

## Disclaimer

DISTRIBUTION STATEMENT A. Approved for public release: distribution unlimited.

© 2013-2016 MASSACHUSETTS INSTITUTE OF TECHNOLOGY

    Subject to FAR 52.227-11 – Patent Rights – Ownership by the Contractor (May 2014)
    SPDX-License-Identifier: MIT

This material is based upon work supported by the US Air Force under Air Force Contract No. FA8721-05-C-0002 and/or FA8702-15-D-0001. Any opinions, findings, conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the US Air Force.

The software/firmware is provided to you on an As-Is basis
