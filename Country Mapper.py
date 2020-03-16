import json
import errno
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback, InputStreamCallback, OutputStreamCallback

flowFile = session.get()

if (flowFile != None):

    try:
        mydict = {}
        S_COUNTRY_ATT = flowFile.getAttribute('S_COUNTRY_MERGED')
        FilePath = CountryMap.evaluateAttributeExpressions(flowFile).getValue()
        S_COUNTRY_LIST = S_COUNTRY_ATT.split(';')
        globalRegions_List =[]
        globalCountries_List =[]
        reader ={}

        with open(str(FilePath),"r") as access_json:
            #reader = csv.DictReader(access_csv)
            reader = json.load(access_json)

        for i in S_COUNTRY_LIST:
            value = reader[i].split(";")
            globalCountries_List.append(value[0])
            globalRegions_List.append(value[1])

        globalRegions_Set = set(globalRegions_List)
        globalRegions = ';'.join(globalRegions_Set)
        #print (globalRegions)

        globalCountries_Set = set(globalCountries_List)
        globalCountries = ';'.join(globalCountries_Set)
        #print (globalCountries)


        flowFile = session.putAttribute(flowFile, "globalRegions", str(globalRegions))
        flowFile = session.putAttribute(flowFile, "globalCountries", str(globalCountries))
        session.transfer(flowFile, REL_SUCCESS)
        session.commit()
    except IOError as e:
        S=e.__str__()
        flowFile = session.putAttribute(flowFile, "Error", str(S))
        session.transfer(flowFile, REL_FAILURE)
        session.commit()
    except Exception as e:
        S = e.message +' ' +e.__doc__
        flowFile = session.putAttribute(flowFile, "Error", str(S))
        session.transfer(flowFile, REL_FAILURE)
        session.commit()
