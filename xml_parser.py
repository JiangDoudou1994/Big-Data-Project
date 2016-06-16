import xml.sax

class FieldHandler( xml.sax.ContentHandler ):
   def __init__(self):
      self.CurrentData = ""
      self.position = ""
      self.element_name = ""
      self.identifier = ""
      self.datatype = ""
      self.format = ""
      self.length = ""
      self.precision = ""
      self.scale = ""
      self.nullable = ""
      self.default_value = ""
      self.description = ""
      self.rules = ""
      


   def startElement(self, tag, attributes):
      self.CurrentData = tag
      if tag == "field":
         print "*****Field*****"
         


   def endElement(self, tag):
      if self.CurrentData == "position":
         print "position:", self.position
      elif self.CurrentData == "element_name":
         print "element_name:", self.element_name
      elif self.CurrentData == "identifier":
         print "identifier:", self.identifier
      elif self.CurrentData == "datatype":
         print "datatype:", self.datatype
      elif self.CurrentData == "format":
         print "format:", self.format
      elif self.CurrentData == "length":
         print "length:", self.length
      elif self.CurrentData == "precision":
         print "precision:", self.precision
      elif self.CurrentData == "scale":
         print "scale:", self.scale
      elif self.CurrentData == "nullable":
         print "nullable:", self.nullable
      elif self.CurrentData == "default_value":
         print "default_value:", self.default_value
      elif self.CurrentData == "description":
         print "Description:", self.description
      elif self.CurrentData == "rules":
         print "rules:", self.rules
      self.CurrentData = ""


   def characters(self, content):
      if self.CurrentData == "position":
         self.position = content
      elif self.CurrentData == "element_name":
         self.element_name = content
      elif self.CurrentData == "identifier":
         self.identifier = content
      elif self.CurrentData == "datatype":
         self.datatype = content
      elif self.CurrentData == "format":
         self.format = content
      elif self.CurrentData == "length":
         self.length = content
      elif self.CurrentData == "precision":
         self.precision = content
      elif self.CurrentData == "scale":
         self.scale = content
      elif self.CurrentData == "nullable":
         self.nullable = content
      elif self.CurrentData == "default_value":
         self.default_value = content
      elif self.CurrentData == "":
         self.precision = content
      elif self.CurrentData == "description":
         self.description = content
      elif self.CurrentData == "rules":
         self.rules = content
      
  
if ( __name__ == "__main__"):
   

   parser = xml.sax.make_parser()

   parser.setFeature(xml.sax.handler.feature_namespaces, 0)

   Handler = FieldHandler()
   parser.setContentHandler( Handler )
   
   parser.parse("smartbase_meta_sample.xml")
