from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import re
from time import time
from operator import add
from  pyspark.sql.types import DoubleType

def condFunc(u1,u2,m1,m2):
    if u1==u2 and m1!=m2:
        return ((m1,m2), 1)
    

def find_something(k=10,lamda=15):
    start = time()
    sc = SparkContext()
    sqlc = SQLContext(sc)

    textFile = sc.textFile("/home/valkhais/ratings.dat")

    n = sc.textFile("/home/valkhais/users.dat").count()
    data = textFile.map(lambda x: x.split('::')[0:4]).map(lambda (u,m,r,t): (int(m),(u,int(r))))
    
    movies_with_scores = data.map(lambda (m,(u,r)): (m, (1 if r>2.5 else 0, 1 if r<3 else 0, 1)))\
			   	 .reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1], x[2]+y[2]))
    
    both_pos_data = data.filter(lambda (m,(u,r)): r>2.5)    
    
    data_with_agg = both_pos_data.join(movies_with_scores)
    
    x_data_with_agg = data_with_agg.filter(lambda (m1,((u1,r1),(p1,n1,t1))): p1>lamda)    
    
    # change the representation
    x_data = x_data_with_agg.map(lambda (m1,((u1,r1),(p1,n1,t1))): (u1,((m1,r1),(p1,n1,t1))))
    all_data = data_with_agg.map(lambda (m2,((u2,r2),(p2,n2,t2))): (u2,((m2,r2),(p2,n2,t2))))
    
    cross = x_data.join(all_data).filter(lambda (u,(((m1,r1),(p1,n1,t1)),(((m2,r2),(p2,n2,t2))))): m1!=m2)\
			         .map(lambda (u,(((m1,r1),(p1,n1,t1)),((m2,r2),(p2,n2,t2)))): ((m1,m2),(1,p1,p2)))\
			         .reduceByKey(lambda x,y: (x[0]+y[0],x[1],x[2]))

    PosLift = cross.map(lambda ((m1,m2),(b,p1,p2)): (m1,m2,float(n) * float(b)/float((p1*p2))))    
    MapedPosLift = PosLift.map(lambda (x,y,p): (x,([(y,p)])))
    ReducedPosLift = MapedPosLift.reduceByKey(lambda x,y: (x+y))
    FinalPosLift = ReducedPosLift.map(lambda x: (x[0], sorted(x[1],key=lambda u: u[1],reverse=True)[:k]))\
			         .flatMap(lambda data: [(data[0], e) for e in data[1]])\
				 .map(lambda (x,(y,p)): str(x) + ',' + str(y) + ',' + str(p) + '\n')
    with open("/home/valkhais/result.csv", 'w') as f:
        for item in FinalPosLift.collect():
	    f.write(item)
    
    print FinalPosLift.top(10)
    print time()-start
    return
   
if __name__ == '__main__':
    find_something(10, 15)
    
