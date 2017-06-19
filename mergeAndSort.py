count1=0
f=open('output200')
k=f.readline()
print k[0:-1]
while(count1<99):
    k=f.readline()
    print k[0:-1]
    count1=count1+1
f.close()


