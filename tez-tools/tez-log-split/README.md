<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Tez log splitter
=========

This is a post-hoc analysis tool for Apache Tez which splits
an aggregated yarn log file to separate files into a hierarchical folder structure.


To use the tool, run e.g.

`tez-log-splitter.sh application_1576254620247_0010`
`tez-log-splitter.sh ~/path/to/application_1576254620247_0010.log`
`tez-log-splitter.sh ~/path/to/application_1576254620247_0010.log.gz`
