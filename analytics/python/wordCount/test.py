#   Copyright 2018 Koverse, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from koverse.transformTest import PySparkTransformTestRunner

from transform import PySparkTransform

import unittest


class TestWordCountTransform(unittest.TestCase):

    def test_count_words(self):
        text = [
            "There is a single instance of the word one",
            "Unlike three there are two instances of the word two",
            "There are three instances of the word three"
        ]

        input_datasets = {'inputDatasets':[{'text': t} for t in text]}
        runner = PySparkTransformTestRunner({'text_field': 'text'}, PySparkTransform)
        output_rdd = runner.testOnLocalData(input_datasets, named=True)

        output = output_rdd.collect()

        self.assertTrue('word' in output[0])
        self.assertTrue('count' in output[0])

        ones, twos, threes = 0, 0, 0

        for rec in output:
            if rec['word'] == 'one':
                ones = rec['count']
            if rec['word'] == 'two':
                twos = rec['count']
            if rec['word'] == 'three':
                threes = rec['count']

        self.assertEqual(ones, 1)
        self.assertEqual(twos, 2)
        self.assertEqual(threes, 3)

if __name__ == '__main__':
    unittest.main()