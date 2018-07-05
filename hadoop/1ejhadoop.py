#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep
import time

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
         "mapreduce.job.name":"1ejercicio.py"
    } 
    MRTrabajo.run()
