#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import dbf
from rutinas.varias import *  # https://github.com/foxcarlos/rutinas.git
import datetime
import psycopg2

class dbf2pg():
    '''Clase que permite exportar una tabla .dbf de foxpro o visual foxpro
    tanto su estructura como los registros hacia una tabla de una base de datos
    de PostGreSQL'''
    def __init__(self):
        self.nombreTablaDbf = ''
        self.nombreTablaPg = ''
        self.camposValue = ''
        self.tiempoInicial = datetime.datetime.now()

    def pgConectar(self, stringConnect):
        '''Conexion con PostGreSQL - Connect to PosGreSQL Ej:
        host='10.121.6.4' dbname='carlosgarciadb' user='admhc' password='shc21152115'
        '''
        cadConex = stringConnect
        try:
            self.conn = psycopg2.connect(cadConex)
            self.cur = self.conn.cursor()
        except:
            # Obtiene la ecepcion mas reciente
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            # sale del Script e Imprime un error con lo que sucedio.
            sys.exit("Database connection failed!\n ->%s" % (exceptionValue))

    def pgEjecutar(self, sqlParametros):
        '''Metodo que permite ejecutar una sentencia SQL'''
        try:
            self.cur.execute(sqlParametros)
        except:
            # Obtiene la ecepcion mas reciente
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            # sale del Script e Imprime un error con lo que sucedio.
            sys.exit("Database connection failed!\n ->%s" % (exceptionValue))

    def abrirTablaDbf(self):
        '''Metodo que permite abrir una tabla DBF'''
        #Module dbf
        #Tabla de Ejemplo - Sample Table
        self.tablaDbf = dbf.Table(self.nombreTablaDbf)
        self.tablaDbf.open()

    def crearTablaPg(self):
        '''Metodo que permite tomar la estructura de una tabla .DBF y
        crear una tabla en PostgreSQL con la misma estructura '''

        fields = ''
        for campo in self.tablaDbf.field_names:
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
        return crearTabla

    def insertarReg(self):
        '''Metodo que permite tomar los registros de una tabla .DBF
        e insertarlos en una tabla en postgreSQL'''

        separador = ','
        camposValue = separador.join(self.tablaDbf.field_names)
        campo = ''
        valorValue = ''
        listaInsert = []
        for r in self.tablaDbf:
            x = [f for f in  r]
            valorValue = ''
            for l in x:
                #type(1L) es Long type(1) es Entero y type(1.0) es Float
                if type(l) in [type(1L), type(1), type(1.0)]:
                    campo = "{0}".format(l)
                if isinstance(l, str) or isinstance(l, unicode):
                    #Ignoro cualquier caracter extraño
                    l = l.encode('ASCII', errors = 'ignore')
                    #Si el campo contiene un signo de $ lo remplazo por ''
                    l = l.replace('$', '') if '$' in l else l
                    campo = "$${0}$$".format(str(l).strip())
                elif isinstance(l, bool):
                    campo = "{0}".format(l)
                elif isinstance(l, datetime.date):
                    campo = "'{0}'".format(l)
                else:
                    campo = "{0}".format("null")
                valorValue = valorValue + campo +', '
            sqlInsert = 'insert into {0} ({1}) values ({2})'.format(self.nombreTablaPg, camposValue, valorValue[:-2])
            listaInsert.append(sqlInsert)
        return listaInsert

    def procesar(self, crearTabla='', insertarReg=''):
        ''' '''
        self.crearTablaPG = crearTabla
        self.insertarRegPG = insertarReg

        if self.crearTablaPG or self.insertarRegPG:
            cadenaConexion = "host='10.121.6.12' dbname='coromoto' user='admhc' password='shc21152115'"
            self.pgConectar(cadenaConexion)

            if self.crearTablaPG:
                self.pgEjecutar(self.crearTablaPG)

            if self.insertarRegPG:
                for fila in self.insertarRegPG:
                    self.pgEjecutar(fila)
            self.conn.commit()
            self.conn.close()

if __name__ == '__main__':
    app = dbf2pg()

    #Indicar el nombre de la tabla .dbf que se desea exportar
    app.nombreTablaDbf = '/media/serv_coromoto/farmacia/datos/farmacos.dbf'
    app.abrirTablaDbf()

    #Indicar el Nombre de la tabla en PostGreSQL que se desea crear
    #En Mi Caso  codigob es un esquema y farmacos es la tabla en PG
    app.nombreTablaPg = 'codigob.farmacos'
    #x = app.crearTablaPg()
    y = app.insertarReg()
    app.procesar('', y)
    tiempoFinal = datetime.datetime.now()
    tiempoTotal = tiempoFinal - app.tiempoInicial
    print 'Segundos total transcurridos:{0}'.format(tiempoTotal.seconds)
