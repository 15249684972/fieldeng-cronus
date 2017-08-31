from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback
import json

def nextNode(currNode, termPath):
   anchorPath = termPath
   #print('anchorPath: '+anchorPath)
   print('******************** sending: ' + anchorPath)
   requests.post(anchorPath)
   for currIndex, elem in enumerate(currNode['childNodes']):
      newNode = currNode['childNodes'][currIndex]
      if len(newNode['childNodes']) > 0:
         termPath = termPath +'/terms/' + newNode['name']
         #print('branching out to leaf: '+termPath)
         #print('******************** sending: ' + termPath)
         nextNode(newNode, termPath)
      else:
         #print(newNode['name'])
         termPath = termPath +'/terms/' + newNode['name']
         #print('end reached, returning: ' + termPath)
         print('******************** sending: ' + termPath)
         requests.post(termPath)
         return newNode
      #print(newNode['name'])
      termPath = anchorPath +'/terms/' + newNode['name']
      #print('termPath: '+termPath)
      #print('anchorPath: '+anchorPath)
      termPath = anchorPath
      #print('termPath: '+termPath)

class PyStreamCallback(StreamCallback):
   def __init__(self, result):
      self.result = result
   def process(self, instream, outstream):
      outstream.write(self.result)

def fail(flowfile, err):
   flowfile = session.putAttribute(flowfile, 'parse.error', err)
   session.transfer(flowfile, REL_FAILURE)

def process(flowfile):
   # Read flowfile content
   data = {}
   instream = session.read(flowfile)
   if hasattr(parse_module, 'format') and parse_module.format.lower() == 'binary':
      data['content'] = IOUtils.toByteArray(instream)
   else:
      data['content'] = IOUtils.toString(instream, StandardCharsets.UTF_8)
   instream.close()

   rootNode = json.load(data)
   for currIndex, elem in enumerate(rootNode):
      rootPath = ${atlas_url} + '/api/atlas/v1/taxanomies/Catalog/terms/'+rootNode[currIndex]['name']
      nextNode(rootNode[currIndex], rootPath)

flowfile = session.get()
if (flowfile != None): process(flowfile)
