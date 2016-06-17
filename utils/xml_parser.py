try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
import sys


def getNodeValue(node, name):
    return node.find(name).text


def getRulesValue(node, name="rules"):

    filed_rules = []
    for child in node:
        if child.tag == "rules" and child.text.strip() != None:
            for rules in child:
                for rule in rules:
                    filed_rules.append(rule.text)
    return filed_rules


def getXmlData(file_name="smartbase_meta_sample.xml"):
    try:
        tree = ET.parse(file_name)
        meta = tree.getroot()
    except Exception, e:
        print "Error:cannot parse file:", file_name
        sys.exit(1)

    field_list = []

    for field in meta:
        position = getNodeValue(field, "position")
        element_name = getNodeValue(field, 'element_name')
        identifier = getNodeValue(field, 'identifier')
        datatype = getNodeValue(field, 'datatype')
        format = getNodeValue(field, 'format')
        length = getNodeValue(field, 'length')
        precision = getNodeValue(field, 'precision')
        scale = getNodeValue(field, 'scale')
        nullable = getNodeValue(field, 'nullable')
        default_value = getNodeValue(field, 'default_value')
        description = getNodeValue(field, 'description')
        rules = getRulesValue(field, 'rules')

        field = {}

        field['position'], field['element_name'], field['identifier'], \
            field['datatype'], field['format'], field['length'], \
            field['precision'], field['scale'], field['nullable'], \
            field['default_value'], field['description'], field['rules'] = (
            position, element_name, identifier,
            datatype, format, length,
            precision, scale, nullable,
            default_value, description, rules
        )

        field_list.append(field)

    return field_list


def test_load_xml():
    field_list = getXmlData()
    for field in field_list:
        print '-----------------------------------------------------'
        print field

if __name__ == "__main__":
    test_load_xml()
