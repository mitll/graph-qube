# MIT Lincoln Laboratory Graph QuBE

This project contains the source for MIT-LL Graph QuBE, a tool to enable efficient pattern-of-behavior search in data containing entities transacting over time. 

This tool helps to answer the basic question: "If I have an interesting pattern of behavior between some entities-of-interest, can I look through all my data to find other such examples of this type of behavior?" Having such a "pattern of behavior" search capability could support many tasks such as situational awareness and decision support.

Developing technology for pattern-of-behavior search in large, transactional data sets, is a challenging task for both the commercial and academic communities. MIT-LL Graph QuBE marries the strengths of approaches from both these communties to develop an efficient two-stage system for pattern search on transactional data.

Links to the appropriate documentation are given below:

## System Description 

The following links give an overview of the underlying technology used by the Graph QuBE system. These documents provide both technical details as well as some context as to how it compares to other similar technologies. 

>[MIT-LL Graph QuBE Executive Summary] (doc/papers/MITLL_GraphQuBE_ExecutiveSummary.pdf)

>[MIT-LL Graph QuBE System Description] (doc/papers/MITLL_GraphQuBE_SystemDescription.pdf)

## Building and Running on Example Data

The following link outlines how to build, launch and use Graph QuBE (with an example data set) from the distributed source. 

>[MIT-LL Graph QuBE User Manual] (doc/XDATA_UserManual.pdf)

An example class is provided that llustrates how the data ingest process can be done from a sample Bitcoin data set: [BitcoinIngest.java] (src/main/java/mitll/xdata/dataset/bitcoin/ingest/BitcoinIngest.java)

## License

Copyright 2014 MIT Lincoln Laboratory, Massachusetts Institute of Technology 

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License.

You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.