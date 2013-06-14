#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from PySide import QtCore, QtGui
import os
import dbf
from rutinas.varias import *
import datetime
import time

class dbf2pg():
    def __init__(self):
        self.nombreTablaDbf = ''
        self.nombreTablaPg = ''
        self.camposValue = ''
        self.tiempoInicial = datetime.datetime.now()
        self.cadConex = ''
        self.archivoConf = ''
    
    def archivoConfiguracion(self):
        '''Parametros 1 tipo String Ej:archivoConfiguracion('config.conf')
        Permite Cargar el Archivo de Configuracion que se le pase como parametro
        y configurar postgres segun los argumentos que esten declarados dentro de 
        este archivo, tales como host, nombre de base de datos, usuario, y clave'''
        ruta_arch_conf = os.path.dirname(sys.argv[0])
        archivo_configuracion = os.path.join(ruta_arch_conf, self.archivoConf)
        fc = FileConfig(archivo_configuracion)
        host, db, user, clave = fc.opcion_consultar('POSTGRESQL')
        self.cadConex = "host='%s' dbname='%s' user='%s' password='%s'" % (host[1], db[1], user[1], clave[1])

    def pgConectar(self):
        '''Conexion con PostGreSQL - Connect to PosGreSQL'''
        try:
            self.conn = psycopg2.connect(self.cadConex)
            self.cur = self.conn.cursor()
        except:
            # Obtiene la ecepcion mas reciente
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            # sale del Script e Imprime un error con lo que sucedio.
            #sys.exit("Database connection failed!\n ->%s" % (exceptionValue))
            print exceptionValue

    def pgEjecutar(self):
        try:
            self.cur.execute(self.sql_parametros)
            #self.records = self.cur.fetchall()
            #self.devolver = self.records
        except:
            # Obtiene la ecepcion mas reciente
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            # sale del Script e Imprime un error con lo que sucedio.
            #sys.exit("Database connection failed!\n ->%s" % (exceptionValue))
            print exceptionValue

        #self.pg = ConectarPG(self.cadConex)

    def abrirTablaDbf(self):
        #Module dbf
        #Tabla de Ejemplo - Sample Table
        #t = '/media/serv_coromoto/Farmacia/Datos/farmacos.dbf'
        #t = '/media/serv_coromoto/digitado/data/dgtdeta.dbf'
        self.tablaDbf = dbf.Table(self.nombreTablaDbf)
        self.tablaDbf.open()

    def crearTablaPg(self):
        fields = ''
        for campo in self.tablaDbf.field_names:
            #Preparar lo que ira en el Insert - Prepare fields name
            #self.camposValue = self.camposValue + '{0} ,'.format(campo)
            
            #Prepara los campos para hacer el Create Table - Prepare the fields to make the "Create Table"
            tipo, long, long2, tipo2 = self.tablaDbf.field_info(campo)
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
        crearTabla = 'CREATE TABLE {0} ({1})'.format(self.nombreTablaPg, fields[:-1].strip())
        self.pgConectar()
        self.sql_parametros = crearTabla
        self.pgEjecutar()
        self.conn.commit()
        print('Tabla Creada con Exit en PostGreSQL')
        self.conn.close()

    def insertarReg(self):
        separador = ','
        camposValue = separador.join(self.tablaDbf.field_names)
        campo = ''
        valorValue = ''

        for r in self.tablaDbf:
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
                valorValue = valorValue + campo +', '
            sqlInsert = 'insert into {0} ({1}) values ({2})'.format(self.nombreTablaPg, camposValue, valorValue[:-2])
            print sqlInsert
            try:
                self.pgConectar()
                self.sql_parametros = sqlInsert
                self.pgEjecutar()
                self.conn.commit()
                #self.pg.ejecutar(sqlInsert)
                time.sleep(1)
            except:
                print('Error...')
                sys.exit()
        self.pg.conn.commit()

if __name__ == '__main__':
    app = dbf2pg()
    app.archivoConf = 'config.conf'
    app.archivoConfiguracion()
    app.nombreTablaDbf = '/media/serv_coromoto/Suministro/Datos/insumos.dbf'
    app.abrirTablaDbf()
    app.nombreTablaPg = 'insumos'
    #app.crearTablaPg()
    app.insertarReg()
    tiempoFinal = datetime.datetime.now()
    tiempoTotal = tiempoFinal - app.tiempoInicial
    print 'Segundos total transcurridos:{0}'.format(tiempoTotal.seconds)
