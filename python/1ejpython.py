#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import datetime

class MRTrabajo(MRJob):
    SORT_VALUES = True
    def tf_mapper(self, _, line):
        line = line.decode("utf-8").split()
        if len(line)>=2:
            IP=line[0]
            FECHA=line[3]
            Archivo=line[6]
            Exito=line[8]
        if (Exito=='200'):
            fecha= time.mktime(time.strptime(FECHA,'[%d/%b/%Y:%H:%M:%S'))
            yield IP,(fecha,Archivo)

            
    def tf_reducer(self,IP,values):
        T=30*24*60*60
        hora=0
        i=0 
        horas=[]
        archivos=[]
        for fecha,archivo in values:            
            if (fecha-hora>=0 and fecha-hora<T):
               archivos[i-1].append(archivo)
            else:
                hora=fecha
                horas.append([hora])
                i=i+1
                archivos.append([archivo])
           
        for v in range(len(archivos)):
            yield (IP,list(set(archivos[v]))),(horas[v][0],i)
    def reducido(self,key,values):
        hora=0
        T=385000
        for horas,n in values:
            if horas-hora>T:
                hora=horas
                yield key[0],(key[1],horas,n)
    def steps(self):
        return [
            MRStep(mapper = self.tf_mapper,
                   reducer = self.tf_reducer),
            MRStep(reducer = self.reducido)
        ]


if __name__ == '__main__':
    MRTrabajo.JOBCONF = {
         #"mapreduce.job.reduces": 10,
         #"mapreduce.task.io.sort.mb":1200,
         #"mapreduce.map.memory.mb":3000,
         #"mapreduce.map.java.opts":"-Xmx1900M  -XX:+UseSerialGC",
         #"mapreduce.reduce.memory.mb":3000,
         #"mapreduce.reduce.java.opts":"-Xmx1800M  -XX:+UseSerialGC",
         #"mapreduce.task.timeout":0,
         "mapreduce.job.name":"1ejercicio.py"
    } 
    tiempoInicial=time.time()

    
    MRTrabajo.run()


    tiempoFinal=time.time()-tiempoInicial


    hor=(int(tiempoFinal/3600))
    minu=int((tiempoFinal-hor*3600)/60)
    seg=tiempoFinal-((hor*3600)+(minu*60))
    print(str(hor)+"h:"+str(minu)+"m:"+str(seg)+"s en terminar")

