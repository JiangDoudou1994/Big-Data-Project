import random
import time

def changetoeng(mounth):
    mounths={1:'January',2:'February',3:'March',4:'April',5:'May',6:'June',7:'July'
    ,8:'August',9:'September',10:'October',11:'November',12:'December'}
    return mounths[int(mounth)]

def generate_time(temp):
    year=str(random.randint(1900,2016))
    mounth=str(random.randint(1,12))
    if len(mounth)==1:
        mounth='0'+mounth
    day=str(random.randint(1,31))
    if len(day)==1:
        day='0'+day
    if temp==1:
        time=mounth+'/'+day+'/'+year
    if temp==2:
        time=mounth+day+year
    if temp==3:
        time=changetoeng(mounth)+','+day+','+year
    return time

def generate_key():
    return str(random.randint(1,9))

def generate_name():
    name=random.randint(97,122)
    return chr(name)
    
start = time.time()
f=open('testdata.txt','w')
testdata=[]
for i in range(3000):
    temp=random.randint(1,3)
    time1=generate_time(temp)
    time2=generate_time(temp)
    key1=generate_key()
    key2=generate_key()
    name=generate_name()
    testdata.append(key1+' '+time1+' '+key2+' '+name+' '+time2)
m=len(testdata)
for i in range(m):
    if i==0:
        h=testdata[i]+'\n'
    else:
        h+=testdata[i]+'\n'
f.write(h)
end = time.time()
print('use time:%d'%(end-start))
print 'ok'
