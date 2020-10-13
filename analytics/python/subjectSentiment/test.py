from koverse.transformTest import PySparkTransformTestRunner
from transform import PySparkTransform
import unittest

text = '''
I can't stand writing test cases, I really hate it.
On the other hand well-tested code is a pretty great thing to have.'
'''

class TestSubjectSentimentTransform(unittest.TestCase):

    def testExtractSubjectSentiment(self):
        global text
        inputDatasets = {'inputDatasets':[{'text': 'This is a test Sentence'}]}
        runner = PySparkTransformTestRunner({'textField': 'text'}, PySparkTransform)
        output = runner.testOnLocalData(inputDatasets, named=True).collect()

        # check we have the output schema we expect
        self.assertTrue('subject' in output[0])
        self.assertTrue('average sentiment' in output[0])
        
        # check output
        for rec in output:
            if rec['subject'] == 'test cases':
                self.assertTrue(rec['average sentiment'] < 0.0)
            if rec['subject'] == 'pretty great thing':
                self.assertTrue(rec['average sentiment'] > 0.0)
        

if __name__ == '__main__':
    unittest.main()

