#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
#from PyQt4 import QtCore, QtGui
from PySide import QtCore, QtGui
from rutinas.varias import *
import os
import recursos
import dbf

ruta_arch_conf = os.path.dirname(sys.argv[0])
archivo_configuracion = os.path.join(ruta_arch_conf, 'config.conf')
fc = FileConfig(archivo_configuracion)

#Tabla de Ejemplo - Sample Table
t = '/media/serv_coromoto/Farmacia/Datos/farmacos.dbf'

#Module dbf
tabla = dbf.Table(t)
tabla.open()

#Variables
fields = ''
nombreTablaNueva = 'codigob.prueba'
host, db, user, clave = fc.opcion_consultar('POSTGRESQL')
cadconex = "host='%s' dbname='%s' user='%s' password='%s'" % (host[1], db[1], user[1], clave[1])
camposValue = ''

for campo in tabla.field_names:
    #Preparar lo que ira en el Insert
    camposValue = camposValue + '{0} ,'.format(campo)

    #Prepara los campos para hacer el Create Table
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
#print crearTabla

sqlInsert = 'insert into {0} ({1}) ({2})'.format(nombreTablaNueva, camposValue, valorValue)
print sqlInsert

#pg = ConectarPG(cadconex)
#pg.ejecutar(crearTabla)
#pg.conn.commit()

'''lcMensaje = 'Registro Guardaro Satisfactoriamente'  # self.combo.currentText()
msgBox = QtGui.QMessageBox(QtGui.QMessageBox.Information, 'Felicidades',lcMensaje)
msgBox.exec_()'''


