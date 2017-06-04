from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import re
from time import time
from operator import add
from  pyspark.sql.types import DoubleType

def save_rdd_as_single_text_file(rdd_object, file_name):
    with open(file_name, 'w') as f:
        for item in rdd_object.collect():
	    f.write(item)

def calc_lifts(k=10,lamda=15):
    start = time()
    sc = SparkContext()
    sqlc = SQLContext(sc)

    textFile = sc.textFile("/home/valkhais/ratings.dat")

    n = sc.textFile("/home/valkhais/users.dat").count()
    data = textFile.map(lambda x: x.split('::')[0:4]).map(lambda (u,m,r,t): (int(m),(u,int(r))))
    
    movies_with_scores = data.map(lambda (m,(u,r)): (m, (1 if r>2.5 else 0, 1 if r<3 else 0, 1)))\
			   	 .reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1], x[2]+y[2]))
    
    pos_data = data.filter(lambda (m,(u,r)): r>2.5)    
    
    pos_data_with_agg = pos_data.join(movies_with_scores)
    
    x_data_with_agg_for_pos = pos_data_with_agg.filter(lambda (m1,((u1,r1),(p1,n1,t1))): p1>lamda)    
    
    # change the representation
    pos_x_data = x_data_with_agg_for_pos.map(lambda (m1,((u1,r1),(p1,n1,t1))): (u1,((m1,r1),(p1,n1,t1))))
    pos_all_data = pos_data_with_agg.map(lambda (m2,((u2,r2),(p2,n2,t2))): (u2,((m2,r2),(p2,n2,t2))))
    
    pos_cross = pos_x_data.join(pos_all_data).filter(lambda (u,(((m1,r1),(p1,n1,t1)),(((m2,r2),(p2,n2,t2))))): m1!=m2)\
			              .map(lambda (u,(((m1,r1),(p1,n1,t1)),((m2,r2),(p2,n2,t2)))): ((m1,m2),(1,p1,p2)))\
			              .reduceByKey(lambda x,y: (x[0]+y[0],x[1],x[2]))

    PosLift = pos_cross.map(lambda ((m1,m2),(b,p1,p2)): (m1,m2,float(n) * float(b)/float((p1*p2))))    
    
    # post-processing for k filtration and file save
    MapedPosLift = PosLift.map(lambda (x,y,p): (x,([(y,p)])))
    ReducedPosLift = MapedPosLift.reduceByKey(lambda x,y: (x+y))
    FinalPosLift = ReducedPosLift.map(lambda x: (x[0], sorted(x[1],key=lambda u: u[1],reverse=True)[:k]))\
			         .flatMap(lambda data: [(data[0], e) for e in data[1]])\
				 .map(lambda (x,(y,p)): str(x) + ',' + str(y) + ',' + str(p) + '\n')

    save_rdd_as_single_text_file(FinalPosLift, "/home/valkhais/PosLift.csv")
    
    #====================================================== The NegLiftCodeBlock =========================================
    neg_data = data.filter(lambda (m,(u,r)): r<3)   
    neg_data_with_agg = neg_data.join(movies_with_scores)
    x_data_with_agg_for_neg = neg_data_with_agg.filter(lambda (m1,((u1,r1),(p1,n1,t1))): n1>lamda)    
    neg_x_data = x_data_with_agg_for_neg.map(lambda (m1,((u1,r1),(p1,n1,t1))): (u1,((m1,r1),(p1,n1,t1))))
    
    # no need for different all data because we want the y to be possitive so we use "pos_all_data"
    neg_cross = neg_x_data.join(pos_all_data).filter(lambda (u,(((m1,r1),(p1,n1,t1)),(((m2,r2),(p2,n2,t2))))): m1!=m2)\
			                     .map(lambda (u,(((m1,r1),(p1,n1,t1)),((m2,r2),(p2,n2,t2)))): ((m1,m2),(1,n1,p2)))\
		                             .reduceByKey(lambda x,y: (x[0]+y[0],x[1],x[2]))
	
    NegLift = neg_cross.map(lambda ((m1,m2),(b,n1,p2)): (m1,m2,float(n) * float(b)/float((n1*p2))))    
    
    # post-processing for k filtration and file save
    MapedNegLift = NegLift.map(lambda (x,y,p): (x,([(y,p)])))
    ReducedNegLift = MapedNegLift.reduceByKey(lambda x,y: (x+y))
    FinalNegLift = ReducedNegLift.map(lambda x: (x[0], sorted(x[1],key=lambda u: u[1],reverse=True)[:k]))\
	                         .flatMap(lambda data: [(data[0], e) for e in data[1]])\
		                 .map(lambda (x,(y,p)): str(x) + ',' + str(y) + ',' + str(p) + '\n')

    save_rdd_as_single_text_file(FinalNegLift, "/home/valkhais/NegLift.csv")
                             
    print time()-start
    return
   
if __name__ == '__main__':
    calc_lifts(10, 15)
    
