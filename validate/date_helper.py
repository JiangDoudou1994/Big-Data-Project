
import datetime

def parse(date):
    parsedate = None
    formats = ['%m%d%Y', '%m,%d,%Y', '%m/%d/%Y', '"%B %d %Y"']
    for df in formats:

        try:
            parsedate = datetime.datetime.strptime(date, df)
 #           print 'match {0}'.format(df)
        except ValueError, error:
           # print error
            pass
        else:
            break

    if parsedate is None:

 #       print '------------------Wrong date-----------------------------'
 #       print date
 #       print '-----------------------------------------------'
        parsedate = datetime.datetime.now()

    return parsedate
