from textblob import TextBlob
import nltk
nltk.download('punkt')
nltk.download('brown')

class PySparkTransform(object):
	def __init__(self, params):
		self.textField = params['textField']
	
	def execute(self, context):
		
		def extractSentimentPerPhrase(doc):
			blob = TextBlob(doc)
			tuples = []
			for sent in blob.sentences:
				sentiment = sent.sentiment.polarity
				# pair the sentence sentiment with each noun phrase in it 
				for phrase in sent.noun_phrases:
					tuples.append((phrase.string, sentiment))
			return tuples
		
		def average(l):
			return sum(map(float, l)) / float(len(l))
		
		textField = self.textField
			 
		rdd = context.inputRdds['inputDatasets']
		
		# get only the records that have some text
		textRecords = rdd.filter(lambda r: textField in r and len(r[textField]) > 0)
		# extract the text
		textRdd = textRecords.map(lambda r: r[textField])
		# extract subjects and sentiment pairs
		subjectSentiments = textRdd.flatMap(extractSentimentPerPhrase)
		# average sentiment per subject
		subjectAvgSentiment = subjectSentiments.groupByKey().map(lambda t: (t[0], average(t[1])))
		# convert to python dicts
		return subjectAvgSentiment.map(lambda t: {'subject': t[0], 'average sentiment': t[1]})

