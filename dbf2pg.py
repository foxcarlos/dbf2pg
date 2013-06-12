#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
#from PyQt4 import QtCore, QtGui
from PySide import QtCore, QtGui
import os
import dbf
from rutinas.varias import *

ruta_arch_conf = os.path.dirname(sys.argv[0])
archivo_configuracion = os.path.join(ruta_arch_conf, 'config.conf')
fc = FileConfig(archivo_configuracion)

#Tabla de Ejemplo - Sample Table
#t = '/media/serv_coromoto/Farmacia/Datos/farmacos.dbf'
t = '/media/serv_coromoto/digitado/data/dgtdeta.dbf'

#Module dbf
tabla = dbf.Table(t)
tabla.open()

#Variables
fields = ''
nombreTablaNueva = 'codigob.dgtdeta'
camposValue = ''

#Conexion con PostGreSQL - Connect to PosGreSQL
host, db, user, clave = fc.opcion_consultar('POSTGRESQL')
cadconex = "host='%s' dbname='%s' user='%s' password='%s'" % (host[1], db[1], user[1], clave[1])
pg = ConectarPG(cadconex)

for campo in tabla.field_names:
    #Preparar lo que ira en el Insert - Prepare fields name
    camposValue = camposValue + '{0} ,'.format(campo)

    #Prepara los campos para hacer el Create Table - Prepare the fields to make the "Create Table"
    tipo, long, long2, tipo2 = tabla.field_info(campo)
    if tipo == 'C':
        c = '{0} character varying({1}) ,'.format(campo, long)
    elif tipo == 'D':
        c = '{0} date ,'.format(campo)
    elif tipo == 'M':
        c = '{0} text ,'.format(campo)
    elif tipo == 'L':
        c = '{0} boolean ,'.format(campo)
    elif tipo == 'T':
        c = '{0} timestamp without time zone ,'.format(campo)
    elif tipo == 'I':
        c = '{0} integer ,'.format(campo)
    elif tipo == 'N':
        c = '{0} numeric({1},{2}) ,'.format(campo, long, long2)
    fields = fields + c
 
crearTabla = 'CREATE TABLE {0} ({1})'.format(nombreTablaNueva, fields[:-1].strip())
pg.ejecutar(crearTabla)

campo = ''
valorValue = ''

for r in tabla:
    x = [f for f in  r]
    valorValue = ''    
    for l in x:
        #type(1L) es Long type(1) es Entero y type(1.0) es Float
        if type(l) in [type(1L), type(1), type(1.0)]:
            campo = "{0}".format(l)
        if isinstance(l, str) or isinstance(l, unicode):
            l = l.encode('ASCII', errors = 'ignore')
            campo = "'{0}'".format(str(l).strip())
        elif isinstance(l, bool):
            campo = "{0}".format(l)
        elif isinstance(l, datetime.date):
            campo = "'{0}'".format(l)
        else:
            campo = "{0}".format("null")
	print(campo)
        valorValue = valorValue + campo +', '
    sqlInsert = 'insert into {0} ({1}) values ({2})'.format(nombreTablaNueva, camposValue[:-1], valorValue[:-2])
    print sqlInsert
    pg.ejecutar(sqlInsert)

pg.conn.commit()
print('Finalizado')
