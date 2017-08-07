#!/usr/bin/python

#from org.apache.commons.io import IOUtils
#from java.nio.charset import StandardCharsets
#from org.apache.nifi.processor.io import StreamCallback
import json

def nextNode(currNode, termPath):
   anchorPath = termPath
   #print('anchorPath: '+anchorPath)
   print('******************** sending: ' + anchorPath)
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
         return newNode
      #print(newNode['name'])
      termPath = anchorPath +'/terms/' + newNode['name']
      #print('termPath: '+termPath)
      #print('anchorPath: '+anchorPath)
      termPath = anchorPath
      #print('termPath: '+termPath)

with open('/root/miningTaxonomy.json') as json_data:
   rootNode = json.load(json_data)

for currIndex, elem in enumerate(rootNode):
   nextNode(rootNode[currIndex], 'http://hostname:21000/api/atlas/v1/taxanomies/Catalog/terms/'+rootNode[currIndex]['name'])
   #print(rootNode[currIndex]['name'])