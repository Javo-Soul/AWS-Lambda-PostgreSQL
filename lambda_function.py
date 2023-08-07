import json
import sys
import os
import boto3
import decimal
import time
import datetime
import base64
import pytz
from conexionPostgress import conexionPostgres
import pandas
import uuid
import numpy as np
from pandas.io.json import json_normalize
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.types import TypeSerializer
from boto3.dynamodb.conditions import Key, Attr

import warnings
warnings.filterwarnings("ignore")

###### librerias para pruebas ############
ts= TypeSerializer()
td = TypeDeserializer()

######################################################################################
# Conexion a DynamoDB cuenta AWS Analytics
######################################################################################
print('------- client and resource 1 -------------')

def lambda_handler(event, context):
    ######################################################################################
    # Variables funcion lambda
    ######################################################################################
    respuesta               = {}  
    mensaje_final           = []  
    fecha_actual            = datetime.datetime.now()
    fecha_local             = fecha_actual.astimezone(pytz.timezone('America/Santiago'))
    fecha_local_final       = fecha_local.strftime('%Y-%m-%d %H:%M:%S')
    flag_error              = 0

    ######################################################################################
    # Utiles dynamodb
    ######################################################################################
    deserializer    = TypeDeserializer()
    serializer      = TypeSerializer()
    
    ######################################################################################
    # Funcion Decimal Encoder
    ######################################################################################
    class DecimalEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, decimal.Decimal):
                if o % 1 > 0:
                    return float(o)
                else:
                    return int(o)
            return super(DecimalEncoder, self).default(o)
    
    ######################################################################################
    # Cuerpo de la funcion lambda
    ######################################################################################
    concat_muestras = pandas.DataFrame() 
    concat_ensayos  = pandas.DataFrame() 
    concat_control  = pandas.DataFrame()
    control_estado  = 0

    print('Inicio registros totales: %s records.' % str(len(event['Records'])))


    for record in event['Records']:
        try:
            mensaje_tmp1    = json.dumps(record.get('dynamodb').get('NewImage'))
            mensaje_tmp2    = json.dumps(mensaje_tmp1)
            mensaje_tmp3    = json.loads(mensaje_tmp2, parse_float=decimal)
            new_mensaje    = json.loads(mensaje_tmp3)            
            #new_mensaje     = {k: deserializer.deserialize(v) for k, v in mensaje_tmp4.items()} 
                
            mensaje_final.append(new_mensaje)
            ##########################################################################################
            # Obtiene campos para insertar control
            ##########################################################################################
            correlativo      = str(new_mensaje['correlativo'])
            componente       = str(new_mensaje['COMPONENTE'])
            muestra_id       = str(new_mensaje['SK'].replace('MUESTRA#',''))
            id_muestra       = (correlativo + '_' + componente + '_' + muestra_id) 

            print("Item cargado: " + id_muestra)

            ######################################################################################
            # Obtiene todos los datos para construir tablas de fidelidad 
            ######################################################################################
            df_maestro  = json_normalize(new_mensaje) 
            # Reemplazar los strings vacíos por None
            df_maestro.replace('', np.nan, inplace=True)

            ######################################################################################
            # Crea dataframe para tabla muestra
            ######################################################################################
            df_muestra = pandas.DataFrame()  
            df_muestra = df_maestro
            ######################################################################################
            # Agrega atributos si no existen en el item para tabla cabecera
            ######################################################################################
            ## correlativo
            if 'correlativo' not in df_muestra.columns:
                df_muestra['correlativo'] = 'SIN_INFORMACION'
            ## id_muestra
            if 'DATA.muestraId' not in df_muestra.columns:
                df_muestra['DATA.muestraId'] = 'SIN_INFORMACION'
            ## id_deudor
            if 'DATA.cliente.numeroSap' not in df_muestra.columns:
                df_muestra['DATA.cliente.numeroSap'] = -1
            ## deudor
            if 'DATA.cliente.nombre' not in df_muestra.columns:
                df_muestra['DATA.cliente.nombre'] = 'SIN_INFORMACION'
                    
            ## id_solicitante
            if 'DATA.cliente.faena.faenaId' not in df_muestra.columns:
                df_muestra['DATA.cliente.faena.faenaId'] = -1
            ## solicitante
            if 'DATA.cliente.faena.nombre' not in df_muestra.columns:
                df_muestra['DATA.cliente.faena.nombre'] = 'SIN_INFORMACION'
            
            if 'DATA.cliente.faena.informacion_comercial.centroBeneficio' not in df_muestra.columns:
                df_muestra['DATA.cliente.faena.informacion_comercial.centroBeneficio'] = 'SIN_INFORMACION'

            if 'DATA.cliente.faena.informacion_comercial.centroBeneficioNombre' not in df_muestra.columns:
                df_muestra['DATA.cliente.faena.informacion_comercial.centroBeneficioNombre'] = 'SIN_INFORMACION'

            if 'DATA.cliente.faena.informacion_comercial.centroDistribucion' not in df_muestra.columns:
                df_muestra['DATA.cliente.faena.informacion_comercial.centroDistribucion'] = 'SIN_INFORMACION'

            if 'DATA.cliente.faena.informacion_comercial.clienteZona' not in df_muestra.columns:
                df_muestra['DATA.cliente.faena.informacion_comercial.clienteZona'] = 'SIN_INFORMACION'  

            if 'DATA.cliente.faena.informacion_comercial.clienteZonaNombre' not in df_muestra.columns:
                df_muestra['DATA.cliente.faena.informacion_comercial.clienteZonaNombre'] = 'SIN_INFORMACION'

            if 'DATA.cliente.faena.direccion' not in df_muestra.columns:
                df_muestra['DATA.cliente.faena.direccion'] = 'SIN_INFORMACION' 
                
            ## id_lubricante
            if 'DATA.lubricante.nombre' not in df_muestra.columns:
                df_muestra['DATA.lubricante.lubricanteId'] = -1
            
            ## lubricante
            if 'DATA.lubricante.nombre' not in df_muestra.columns:
                df_muestra['DATA.lubricante.nombre'] = 'SIN_INFORMACION'
            ## id_componente
            if 'DATA.componente.componenteId' not in df_muestra.columns:
                df_muestra['DATA.componente.componenteId'] = 'SIN_INFORMACION'
            ## componente
            if 'DATA.componente.descriptor' not in df_muestra.columns:
                df_muestra['DATA.componente.descriptor'] = 'SIN_INFORMACION'
            if 'DATA.componente.marca' not in df_muestra.columns:
                df_muestra['DATA.componente.marca'] = 'SIN_INFORMACION'
            if 'DATA.componente.modelo' not in df_muestra.columns:
                df_muestra['DATA.componente.modelo'] = 'SIN_INFORMACION'
            if 'DATA.componente.tipoComponente.nombre' not in df_muestra.columns:
                df_muestra['DATA.componente.tipoComponente.nombre'] = 'SIN_INFORMACION'
            if 'DATA.componente.tipoComponente.tipoComponenteId' not in df_muestra.columns:
                df_muestra['DATA.componente.tipoComponente.tipoComponenteId'] = 'SIN_INFORMACION'
                
            ## 7. id_equipo  
            if 'DATA.componente.equipo.equipoId' not in df_muestra.columns:
                df_muestra['DATA.componente.equipo.equipoId'] = 'SIN_INFORMACION'
            if  'DATA.componente.equipo.nombre'  not in df_muestra.columns:
                df_muestra[ 'DATA.componente.equipo.nombre' ] = 'SIN_INFORMACION'
            if 'DATA.componente.equipo.marca' not in df_muestra.columns:
                df_muestra['DATA.componente.equipo.marca'] = 'SIN_INFORMACION'
            if 'DATA.componente.equipo.modelo' not in df_muestra.columns:
                df_muestra['DATA.componente.equipo.modelo'] = 'SIN_INFORMACION'
            ## 8. id_tipo_equipo
            if 'DATA.componente.equipo.tipoEquipo.tipoEquipoId' not in df_muestra.columns:
                df_muestra['DATA.componente.equipo.tipoEquipo.tipoEquipoId'] = -1
            ##  tipo_equipo
            if 'DATA.componente.equipo.tipoEquipo.nombre' not in df_muestra.columns:
                df_muestra['DATA.componente.equipo.tipoEquipo.nombre'] = 'SIN_INFORMACION'
            ## 9. laboratorio
            if 'LABORATORIO' not in df_muestra.columns:
                df_muestra['LABORATORIO'] = 'SIN_INFORMACION'
            ## 10. Lote
            if 'LOTE' not in df_muestra.columns:
                df_muestra['LOTE'] = 'SIN_INFORMACION'
            ## 11. id_plan_analisis: DATA.planAnalisis.planAnalisisId
            if 'DATA.planAnalisis.planAnalisisId' not in df_muestra.columns:
                df_muestra['DATA.planAnalisis.planAnalisisId'] = 'SIN_INFORMACION'
            ## 12. nombre_plan_analisis: DATA.planAnalisis.nombre
            if 'DATA.planAnalisis.nombre' not in df_muestra.columns:
                df_muestra['DATA.planAnalisis.nombre'] = 'SIN_INFORMACION'
            ## 13. id_solicitud_analisis: SOLICITUD_ANALISIS
            if 'SOLICITUD_ANALISIS' not in df_muestra.columns:
                df_muestra['SOLICITUD_ANALISIS'] = 'SIN_INFORMACION'
            ## 14. Resolucion: DATA.resolucion
            if 'DATA.resolucion' not in df_muestra.columns:
                df_muestra['DATA.resolucion'] = 'SIN_INFORMACION'
            ## 15. comentario: DATA.comentarios.comentario
            if 'DATA.comentarios.comentario' not in df_muestra.columns:
                df_muestra['DATA.comentarios.comentario'] = 'SIN_INFORMACION'
            ## 16. fecha_informe:  DATA.fechaInforme
            if 'DATA.fechaInforme' not in df_muestra.columns:
                df_muestra['DATA.fechaInforme'] = '1970-01-01T00:00:00.000000'
            ## 17. fecha_ingreso: 'DATA.fechaIngreso'
            if 'DATA.fechaIngreso' not in df_muestra.columns:
                df_muestra['DATA.fechaIngreso'] = '1970-01-01T00:00:00.000000'
            
            ## 18. fecha_muestreo: 'DATA.fechaMuestreo'  
            if 'DATA.fechaMuestreo' not in df_muestra.columns:
                df_muestra['DATA.fechaMuestreo'] = '1970-01-01T00:00:00.000000' 
            ## 19. cantidad_rellenos 'DATA.rellenoDesdeUltimoCambio'
            if 'DATA.rellenoDesdeUltimoCambio' not in df_muestra.columns:
                df_muestra['DATA.rellenoDesdeUltimoCambio'] = -1 
            ## 20. uso_cambio_lubricante 'DATA.usoCambioLubricante'  
            if 'DATA.usoCambioLubricante' not in df_muestra.columns:
                df_muestra['DATA.usoCambioLubricante'] = -1 
            ## 21. uso_total_componente 'DATA.usoTotalComponente'
            if 'DATA.usoTotalComponente' not in df_muestra.columns:
                df_muestra['DATA.usoTotalComponente'] = -1 
            ## 17. fecha_recepcion: 'DATA.fechaRecepcion'
            if 'DATA.fechaRecepcion' not in df_muestra.columns:
                df_muestra['DATA.fechaRecepcion'] = '1970-01-01T00:00:00.000000'

            ######################################################################################
            # Renombra atributos de la tabla cabecera
            ######################################################################################
            df_muestra        = df_muestra.rename(
                        columns={
                            # 1
                            'correlativo'                                                       : 'correlativo',
                            # 2
                            'DATA.muestraId'                                                    : 'id_muestra_frasco',
                            # 3
                            'DATA.cliente.numeroSap'                                            : 'id_deudor',
                            'DATA.cliente.nombre'                                               : 'deudor',
                            # 4
                            'DATA.cliente.faena.faenaId'                                        : 'id_solicitante',
                            'DATA.cliente.faena.nombre'                                         : 'solicitante',
                            'DATA.cliente.faena.informacion_comercial.centroBeneficio'          : 'id_centro_beneficio',
                            'DATA.cliente.faena.informacion_comercial.centroBeneficioNombre'    : 'centro_beneficio',
                            'DATA.cliente.faena.informacion_comercial.centroDistribucion'       : 'id_centro_distribucion',
                            'DATA.cliente.faena.informacion_comercial.clienteZona'              : 'id_jefe_zona',
                            'DATA.cliente.faena.informacion_comercial.clienteZonaNombre'        : 'jefe_zona',
                            'DATA.cliente.faena.direccion'                                      : 'direccion_cliente',
                            # 5
                            'DATA.lubricante.lubricanteId'                                      : 'id_lubricante',
                            'DATA.lubricante.nombre'                                            : 'lubricante_nombre',
                            # 6
                            'DATA.componente.componenteId'                                      : 'id_componente',
                            'DATA.componente.descriptor'                                        : 'componente_descriptor',
                            'DATA.componente.marca'                                             : 'marca_componente',
                            'DATA.componente.modelo'                                            : 'modelo_componente',
                            'DATA.componente.tipoComponente.nombre'                             : 'tipo_componente',
                            'DATA.componente.tipoComponente.tipoComponenteId'                   : 'id_tipo_componente',
                            # 7
                            'DATA.componente.equipo.equipoId'                                   : 'id_equipo',
                            
                            'DATA.componente.equipo.nombre'                                     : 'equipo',
                            'DATA.componente.equipo.marca'                                      : 'marca_equipo',
                            'DATA.componente.equipo.modelo'                                     : 'modelo_equipo',
                            # 8
                            'DATA.componente.equipo.tipoEquipo.tipoEquipoId'                    : 'id_tipo_equipo',
                            'DATA.componente.equipo.tipoEquipo.nombre'                          : 'tipo_equipo',
                            
                            # 9
                            'LABORATORIO'                                                       : 'laboratorio',
                            # 10
                            'LOTE'                                                              : 'lote',
                            # 11
                            'DATA.planAnalisis.planAnalisisId'                                  : 'id_plan_analisis',
                            # 12
                            'DATA.planAnalisis.nombre'                                          : 'nombre_plan_analisis',
                            # 13
                            'SOLICITUD_ANALISIS'                                                : 'id_solicitud_analisis',
                            # 14
                            'DATA.resolucion'                                                   : 'resolucion',
                            # 15
                            'DATA.comentarios.comentario'                                       : 'comentario',
                            # 16
                            'DATA.fechaInforme'                                                 : 'fecha_informe',
                            # 17
                            'DATA.fechaIngreso'                                                 : 'fecha_solicitud',
                            'DATA.fechaRecepcion'                                               : 'fecha_ingreso',
                            # 18
                            'DATA.fechaMuestreo'                                                : 'fecha_muestreo',
                            # 19
                            'DATA.rellenoDesdeUltimoCambio'                                     : 'cantidad_rellenos',
                            # 20
                            'DATA.usoCambioLubricante'                                          : 'uso_cambio_lubricante',
                            # 21
                            'DATA.usoTotalComponente'                                           : 'uso_total_componente'
                        })
                        
            ######################################################################################
            # Calidad de datos para tabla cabecera
            ######################################################################################
            
            df_muestra.fillna({
                            # 1
                            'correlativo'                       : 'SIN_INFORMACION',
                            # 2
                            'id_muestra_frasco'                 : 'SIN_INFORMACION',
                            # 3
                            'id_deudor'                         : -1,
                            'deudor'                            : 'SIN_INFORMACION',
                            # 4
                            'id_solicitante'                    : -1,
                            'solicitante'                       : 'SIN_INFORMACION',
                            'id_centro_beneficio'               : 'SIN_INFORMACION',
                            'centro_beneficio'                  : 'SIN_INFORMACION',
                            'id_centro_distribucion'            : 'SIN_INFORMACION',
                            'id_jefe_zona'                      : 'SIN_INFORMACION',
                            'jefe_zona'                         : 'SIN_INFORMACION',
                            'direccion_cliente'                 : 'SIN_INFORMACION',
                            # 5
                            'id_lubricante'                     : -1,
                            'lubricante_nombre'                 : 'SIN_INFORMACION',
                            # 6
                            'id_componente'                     : 'SIN_INFORMACION',
                            'componente_descriptor'             : 'SIN_INFORMACION',
                            'marca_componente'                  : 'SIN_INFORMACION',
                            'modelo_componente'                 : 'SIN_INFORMACION',
                            'tipo_componente'                   : 'SIN_INFORMACION',
                            'id_tipo_componente'                : 'SIN_INFORMACION',
                            # 7
                            'id_equipo'                         : 'SIN_INFORMACION',
                            'equipo'                            : 'SIN_INFORMACION',
                            'marca_equipo'                      : 'SIN_INFORMACION',
                            'modelo_equipo'                     : 'SIN_INFORMACION',
                            # 8
                            'id_tipo_equipo'                    : -1,
                            'tipo_equipo'                       : 'SIN_INFORMACION',
                            # 9
                            'laboratorio'                       : 'SIN_INFORMACION',
                            # 10
                            'lote'                              : 'SIN_INFORMACION',
                            # 11
                            'id_plan_analisis'                  : 'SIN_INFORMACION',
                            # 12
                            'nombre_plan_analisis'              : 'SIN_INFORMACION',
                            # 13
                            'id_solicitud_analisis'             : 'SIN_INFORMACION',
                            # 14
                            'resolucion'                        : 'SIN_INFORMACION',
                            # 15
                            'comentario'                        : 'SIN_INFORMACION',
                            # 16
                            'fecha_informe'                     : '1970-01-01T00:00:00.000000',
                            # 17
                            'fecha_solicitud'                   : '1970-01-01T00:00:00.000000',
                            'fecha_ingreso'                     : '1970-01-01T00:00:00.000000',
                            # 18
                            'fecha_muestreo'                    : '1970-01-01T00:00:00.000000',
                            # 19
                            'cantidad_rellenos'                 : -1,
                            # 20
                            'uso_cambio_lubricante'             : -1,
                            # 21
                            'uso_total_componente'              : -1
                    
            }, inplace=True)
            ######################################################################################
            # Construccion tabla muestra
            ######################################################################################
            # 1
            df_muestra['correlativo']               = df_muestra['correlativo'].astype('str').str.upper()
            # 2
            df_muestra['id_muestra_frasco']         = df_muestra['id_muestra_frasco'].astype('str').str.upper()
            # 3
            df_muestra['id_deudor']                 = df_muestra['id_deudor'].astype('int')
            df_muestra['deudor']                    = df_muestra['deudor'].astype('str').str.upper()
            # 4
            df_muestra['id_solicitante']            = df_muestra['id_solicitante'].astype('int')
            df_muestra['solicitante']               = df_muestra['solicitante'].astype('str').str.upper()
            df_muestra['id_centro_beneficio']       = df_muestra['id_centro_beneficio'].astype('str').str.upper()
            df_muestra['centro_beneficio']          = df_muestra['centro_beneficio'].astype('str').str.upper()
            df_muestra['id_centro_distribucion']    = df_muestra['id_centro_distribucion'].astype('str').str.upper()
            df_muestra['id_jefe_zona']              = df_muestra['id_jefe_zona'].astype('str').str.upper()
            df_muestra['jefe_zona']                 = df_muestra['jefe_zona'].astype('str').str.upper()
            df_muestra['direccion_cliente']         = df_muestra['direccion_cliente'].astype('str').str.upper()
            # 5
            df_muestra['id_lubricante']             = df_muestra['id_lubricante'].astype('int')
            df_muestra['lubricante_nombre']         = df_muestra['lubricante_nombre'].astype('str').str.upper()
            # 6
            df_muestra['id_componente']             = df_muestra['id_componente'].astype('str').str.upper()
            df_muestra['componente_descriptor']     = df_muestra['componente_descriptor'].astype('str').str.upper()
            df_muestra['marca_componente']          = df_muestra['marca_componente'].astype('str').str.upper()
            df_muestra['modelo_componente']         = df_muestra['modelo_componente'].astype('str').str.upper()
            df_muestra['tipo_componente']           = df_muestra['tipo_componente'].astype('str').str.upper()
            df_muestra['id_tipo_componente']        = df_muestra['id_tipo_componente'].astype('str').str.upper()
            # 7
            df_muestra['id_equipo']                 = df_muestra['id_equipo'].astype('str').str.upper()
            df_muestra['equipo']                    = df_muestra['equipo'].astype('str').str.upper()
            df_muestra['marca_equipo']              = df_muestra['marca_equipo'].astype('str').str.upper()
            df_muestra['modelo_equipo']             = df_muestra['modelo_equipo'].astype('str').str.upper()
            # 8
            df_muestra['id_tipo_equipo']            = df_muestra['id_tipo_equipo'].astype('int')
            df_muestra['tipo_equipo']               = df_muestra['tipo_equipo'].astype('str').str.upper()
            # 9
            df_muestra['laboratorio']               = df_muestra['laboratorio'].astype('str').str.upper()
            # 10
            
            df_muestra['lote']                      = df_muestra['lote'].astype('str').str.upper()
            # 11
            df_muestra['id_plan_analisis']          = df_muestra['id_plan_analisis'].astype('str').str.upper()
            # 12
            df_muestra['nombre_plan_analisis']      = df_muestra['nombre_plan_analisis'].astype('str').str.upper()
            # 13
            df_muestra['id_solicitud_analisis']     = df_muestra['id_solicitud_analisis'].astype('str').str.upper()
            # 14
            df_muestra['resolucion']                = df_muestra['resolucion'].astype('str').str.upper()
            # 15
            df_muestra['comentario']                = df_muestra['comentario'].astype('str').str.upper()
            # 16
            df_muestra['fecha_informe']             = df_muestra['fecha_informe'].astype("datetime64[s]")
            # 17
            df_muestra['fecha_ingreso']             = df_muestra['fecha_ingreso'].astype("datetime64[s]")
            # 18
            df_muestra['fecha_muestreo']            = df_muestra['fecha_muestreo'].astype("datetime64[s]")
            df_muestra['fecha_solicitud']           = df_muestra['fecha_solicitud'].astype("datetime64[s]")
            # 19 
            df_muestra['cantidad_rellenos']         = df_muestra['cantidad_rellenos'].astype('str').str.upper()
            # 20
            df_muestra['uso_cambio_lubricante']     = df_muestra['uso_cambio_lubricante'].astype('str').str.upper()
            # 21
            df_muestra['uso_total_componente']      = df_muestra['uso_total_componente'].astype('str').str.upper()
            ## Se agrega fecga de carga
            df_muestra['fecha_carga']               = fecha_local_final
            df_muestra['fecha_carga']               = df_muestra['fecha_carga'].astype("datetime64[s]")
            ######################################################################################
            # Selecciona y ordena las columnas para tabla cabecera
            ######################################################################################
            df_muestra    = df_muestra[[
                        'correlativo',
                        'id_muestra_frasco',
                        'id_deudor', 
                        'deudor',
                        'id_solicitante',
                        'solicitante',
                        'id_centro_beneficio',
                        'centro_beneficio',
                        'id_centro_distribucion',
                        'id_jefe_zona',
                        'jefe_zona',
                        'direccion_cliente',
                        'id_lubricante',
                        'lubricante_nombre',
                        'id_componente',
                        'componente_descriptor',
                        'marca_componente',
                        'modelo_componente',
                        'tipo_componente',
                        'id_tipo_componente',
                        'id_equipo',
                        'equipo',
                        'marca_equipo',
                        'modelo_equipo',
                        'id_tipo_equipo',
                        'tipo_equipo',
                        'laboratorio',
                        'lote',
                        'id_plan_analisis',
                        'nombre_plan_analisis',
                        'id_solicitud_analisis',
                        'resolucion',
                        'comentario',
                        'fecha_informe',
                        'fecha_ingreso',
                        'fecha_muestreo',
                        'fecha_solicitud',
                        'cantidad_rellenos',
                        'uso_cambio_lubricante',
                        'uso_total_componente',
                        'fecha_carga']]
            ######################################################################################
            # Carga tabla muestra en Landing S3
            ######################################################################################
            concat_muestras = concat_muestras.append(df_muestra, ignore_index=True)

            concat_muestras        = concat_muestras.rename(
                columns={'nombre_plan_analisis'         : 'plan_analisis',
                            'id_muestra_frasco'         : 'id_muestra'
                        })

            print('Muestra registrada OK')

            ######################################################################################
            # Crea dataframe para tabla ensayo
            ######################################################################################  
            df_ensayo = pandas.DataFrame()  
            df_ensayo = df_maestro
            
            ## Agregar atributo si no existe el dato de cabecera para entidad ensayos: 
            if (('DATA.resultados' in df_ensayo.columns) and (df_ensayo['DATA.resultados'] is not None)):
                df_ensayo = df_ensayo[['correlativo','COMPONENTE','DATA.fechaMuestreo','DATA.resultados']]
                
                df_ensayo_final = pandas.DataFrame()
                df_ensayo_acum = pandas.DataFrame()
                
                df_ensayo = df_ensayo[~df_ensayo['DATA.resultados'].isna()]
                for index, fila_1 in df_ensayo.iterrows():
                    
                    df_ensayo_tmp = json_normalize(fila_1['DATA.resultados'])
                
                    ## cabecera del ensayo
                    correlativo_ensayo          = fila_1['correlativo']
                    componente_ensayo           = fila_1['COMPONENTE']
                    fecha_muestreo_ensayo       = fila_1['DATA.fechaMuestreo']
                    ## 0 id_muestra 
                    if 'muestraId' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['muestraId'] = 'SIN_INFORMACION'  
                    ## 2. id_ensayo
                    if 'ensayo.ensayoId' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['ensayo.ensayoId'] = -1
                    ## 2.2 codigo_ensayo
                    if 'ensayo.code' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['ensayo.code'] = 'SIN_INFORMACION' 
                        
                    ## 3. ensayo
                    if 'ensayo.nombre' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['ensayo.nombre'] = 'SIN_INFORMACION' 

                    ## 3. tipo_ensayo
                    if 'tipo' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['tipo'] = 'SIN_INFORMACION' 

                    ## 4. fecha_ensayo
                    if 'fecha' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['fecha'] = '1970-01-01T00:00:00.000000'

                    ## 5. hora_ensayo
                    if 'hora' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['hora'] = 'SIN_INFORMACION'

                    ## 6. id_resultado
                    if 'resultadoId' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['resultadoId'] = 'SIN_INFORMACION'

                    ## 7. stamp
                    if 'stamp' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['stamp'] = 'SIN_INFORMACION'

                    ## 8. valor
                    if 'value' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['value'] = 'SIN_INFORMACION' 

                    ## 9. id_usuario  
                    if 'usuario.userId' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['usuario.userId'] = 'SIN_INFORMACION'

                    ## 10. usuario 
                    if 'usuario.nombre' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['usuario.nombre'] = 'SIN_INFORMACION'
                        
                    ## 11. estado_color
                    if 'estado.color' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['estado.color'] = 'SIN_INFORMACION'    

                    ## 12. estatus
                    if 'estado.status' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['estado.status'] = 'SIN_INFORMACION'  

                    ## 13. estado_limite_min
                    if 'estado.limites.min' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['estado.limites.min'] = -1

                    ## 14. estado_limite_max
                    if 'estado.limites.max' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['estado.limites.max'] = -1

                    ## 15. estado_limite_allow_min
                    if 'estado.limites.allowMin' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['estado.limites.allowMin'] = -1

                    ## 16. estado_limite_allow_max
                    if 'estado.limites.allowMax' not in df_ensayo_tmp.columns:
                        df_ensayo_tmp['estado.limites.allowMax'] = -1

                                                                                                                                                                                                                                                                                                                                                                        
                    ## Rename 
                    for index, fila_2 in df_ensayo_tmp.iterrows():
                        df_ensayo_acum = df_ensayo_acum.append({
                            
                            # 1. correlativo
                            'correlativo'                : correlativo_ensayo,
                            'componente'                 : componente_ensayo,
                                
                            'fecha_muestreo'             : fecha_muestreo_ensayo,
                                
                            'muestraID'                  : fila_2['muestraId'],
                                
                            'codigo_ensayo'              : fila_2['ensayo.code'],
                            # 2. id_ensayo: ensayo.ensayoId
                            'id_ensayo'                  : fila_2['ensayo.ensayoId'],

                            # 2,5. ensayo: ensayo.nombre
                            'ensayo'                     : fila_2['ensayo.nombre'],

                            # 3. tipo_ensayo: tipo
                            'tipo_ensayo'                : fila_2['tipo'],

                            # 4. fecha_ensayo: fecha
                            'fecha_ensayo'               : fila_2['fecha'],

                            # 5. hora_ensayo: hora
                            'hora_ensayo'                : fila_2['hora'],

                            # 6. id_resultado: resultadoId
                            'id_resultado'               : fila_2['resultadoId'],

                            # 7. stamp: stamp
                            'stamp'                      : fila_2['stamp'],

                            # 8. valor: value
                            'valor'                      : fila_2['value'],

                            # 9. id_usuario: usuario.userId
                            'id_usuario'                 : fila_2['usuario.userId'],

                            # 10. usuario: usuario.nombre
                            'usuario'                    : fila_2['usuario.nombre'],

                            # 11. estado_color: estado.color
                            'estado_color'               : fila_2['estado.color'],

                            # 12. estatus: estado.status
                            'estatus'                    : fila_2['estado.status'],

                            # 13. estado_limite_min: estado.limites.min
                            'estado_limite_min'          : fila_2['estado.limites.min'],

                            # 14. estado_limite_max: estado.limites.max
                            'estado_limite_max'          : fila_2['estado.limites.max'],

                            # 15. estado_limite_allow_min: estado.limites.allowMin
                            'estado_limite_allow_min'    : fila_2['estado.limites.allowMin'],

                            # 16. estado_limite_allow_max: estado.limites.allowMax
                            'estado_limite_allow_max'    : fila_2['estado.limites.allowMax']}, ignore_index=True)     
                            
                            
                    df_ensayo_final = df_ensayo_final.append(df_ensayo_acum, ignore_index=True)
                
            
            
                ######################################################################################
                # Calidad de datos para tabla ensayo
                ###################################################################################### 
                df_ensayo_final.replace('', np.nan, inplace=True)
                        
                df_ensayo_final.fillna(        
                            {
                                # 1
                                'correlativo'               :'SIN_INFORMACION',
                                ## Campos agregados para cuadarar con nueva estructura y estructura antiga
                                'componente'                :'SIN_INFORMACION',
                                'fecha_muestreo'            :'1970-01-01T00:00:00.000000',
                                'muestraID'                 :'SIN_INFORMACION',
                                'codigo_ensayo'             :'SIN_INFORMACION',
                                # 2
                                'id_ensayo'                 :-1,
                                # 3
                                'ensayo'                    :'SIN_INFORMACION',
                                # 3
                                'tipo_ensayo'               :'SIN_INFORMACION',
                                # 4
                                'fecha_ensayo'              :'1970-01-01T00:00:00.000000',
                                # 5
                                'hora_ensayo'               :'SIN_INFORMACION',
                                # 6
                                'id_resultado'              :'SIN_INFORMACION',
                                # 7
                                'stamp'                     :'SIN_INFORMACION',
                                # 8
                                'valor'                     :'SIN_INFORMACION',
                                # 9
                                'id_usuario'                :'SIN_INFORMACION',
                                # 10
                                'usuario'                   :'SIN_INFORMACION',
                                # 11
                                'estado_color'              :'SIN_INFORMACION',
                                # 12
                                'estatus'                   :'SIN_INFORMACION',
                                # 13
                                'estado_limite_min'         :-1,
                                # 14
                                'estado_limite_max'         :-1,
                                # 15
                                'estado_limite_allow_min'   :-1,
                                # 16
                                'estado_limite_allow_max'   :-1,
            
                    }, inplace=True)
            
                ######################################################################################
                # Construccion tabla muestra
                ######################################################################################
            
                # 1
                df_ensayo_final['correlativo']              = df_ensayo_final['correlativo'].astype('str').str.upper()
                df_ensayo_final['componente']               = df_ensayo_final['componente'].astype('str').str.upper()
                df_ensayo_final['fecha_muestreo']           = df_ensayo_final['fecha_muestreo'].astype("datetime64[s]")
                df_ensayo_final['muestraID']               = df_ensayo_final['muestraID'].astype('str').str.upper()
                df_ensayo_final['codigo_ensayo']            = df_ensayo_final['codigo_ensayo'].astype('str').str.upper()
                # 2
                df_ensayo_final['id_ensayo']                = df_ensayo_final['id_ensayo'].astype('int')
                # 2.5
                df_ensayo_final['ensayo']                   = df_ensayo_final['ensayo'].astype('str').str.upper()
                # 3
                df_ensayo_final['tipo_ensayo']              = df_ensayo_final['tipo_ensayo'].astype('str').str.upper()
                # 4
                df_ensayo_final['fecha_ensayo']             = df_ensayo_final['fecha_ensayo'].astype("datetime64[s]")
                # 5
                df_ensayo_final['hora_ensayo']              = df_ensayo_final['hora_ensayo'].astype('str').str.upper()
                # 6
                df_ensayo_final['id_resultado']             = df_ensayo_final['id_resultado'].astype('str').str.upper()
                # 7
                df_ensayo_final['stamp']                    = df_ensayo_final['stamp'].astype('str').str.upper()
                # 8
                df_ensayo_final['valor']                    = df_ensayo_final['valor'].astype('str').str.upper()
                # 9
                df_ensayo_final['id_usuario']               = df_ensayo_final['id_usuario'].astype('str').str.upper()
                # 10
                df_ensayo_final['usuario']                  = df_ensayo_final['usuario'].astype('str').str.upper()
                # 11
                df_ensayo_final['estado_color']             = df_ensayo_final['estado_color'].astype('str').str.upper()
                # 12
                df_ensayo_final['estatus']                  = df_ensayo_final['estatus'].astype('str').str.upper()
                # 13
                df_ensayo_final['estado_limite_min']        = df_ensayo_final['estado_limite_min'].astype('float64').round(2)
                # 14
                df_ensayo_final['estado_limite_max']        = df_ensayo_final['estado_limite_max'].astype('float64').round(2)
                # 15
                df_ensayo_final['estado_limite_allow_min']  = df_ensayo_final['estado_limite_allow_min'].astype('float64').round(2)
                # 16
                df_ensayo_final['estado_limite_allow_max']  = df_ensayo_final['estado_limite_allow_max'].astype('float64').round(2)
                # 17 fecha carga
                df_ensayo_final['fecha_carga']              = fecha_local_final
                df_ensayo_final['fecha_carga']              = df_ensayo_final['fecha_carga'].astype("datetime64[s]")
                ######################################################################################
                # Selecciona y ordena las columnas para tabla cabecera
                ######################################################################################
                df_ensayo_final  = df_ensayo_final[['correlativo','componente','codigo_ensayo','muestraID','id_ensayo'
                ,'ensayo','tipo_ensayo','fecha_muestreo','fecha_ensayo'
                ,'hora_ensayo','id_resultado','stamp','valor','id_usuario','usuario','estado_color','estatus'
                ,'estado_limite_min','estado_limite_max','estado_limite_allow_min','estado_limite_allow_max','fecha_carga']]
                
                ######################################################################################
                # Se genero el dataframe de ensayos
                ######################################################################################
                concat_ensayos = concat_ensayos.append(df_ensayo_final, ignore_index=True)


                concat_ensayos        = concat_ensayos.rename(
                columns={'componente'         : 'id_componente',
                            'muestraID'         : 'id_muestra'  
                        })
                
                print('Ensayo registrado OK')
                
                ##########################################################################################
                # Inserta registro en tabla RDS
                ##########################################################################################

                def queryRDS(query, tipo):
                    response = []
                    try:
                        conexion = conexionPostgres()
                        cursor = conexion.cursor()

                        if tipo == 'select':        
                                cursor.execute(query)
                                response = cursor.fetchone()
                                cursor.close()

                        elif tipo == 'insert':
                                cursor.execute(query)
                                cursor.close()
                                response = 'Inserto datos Exitosamente'

                        elif tipo == 'update':
                                cursor.execute(query)
                                cursor.close()
                                response = 'Actualizo datos Exitosamente'
                        else:
                            print('Accion no Valida')

                    except Exception as e:
                        response = 'Error aca'+ str(e)
                    return response

                ##########################################################################################
                    # Inserta muestras en RDS
                ##########################################################################################
                Q = int(concat_muestras['id_muestra'].count())

                for i in range(Q):
                    columnas = []
                    columnas1 = concat_muestras.columns.values
                    columnas = ','.join(columnas1)
                    valores = []
                    fila =  list(concat_muestras.iloc[i])

                    for item in fila:
                            valores.append("'"+str(item)+"'")

                    valores = ','.join(map(str, valores))

                    selectData = f'''SELECT correlativo, id_muestra FROM copec_lims_prd.muestra_rl
                                WHERE correlativo = '{fila[0]}' and id_muestra = '{fila[1]}' '''

                    if  queryRDS(selectData,'select') == None:
                        insertData = f''' insert into copec_lims_prd.muestra_rl({(columnas)})
                                                                    values ({(valores)})'''

                        result = queryRDS(insertData,'insert')

                    else:
                        i = 0
                        valores = []
                        for campos in columnas1:
                            valores.append(f''' "{campos}" = '{fila[i]}' ''')
                            i += 1

                        valores = ','.join(map(str, valores))

                        updateData = f''' update copec_lims_prd.muestra_rl set {valores}
                                            where correlativo = '{fila[0]}' and id_muestra = '{fila[1]}' '''
                        result = queryRDS(updateData,'update')

                    print(result)

                ##########################################################################################
                ################## Inserta 'ensayos' en RDS
                Q = int(concat_ensayos['id_ensayo'].count())

                for i in range(Q):
                    columnas = []
                    columnas1 = concat_ensayos.columns.values
                    columnas = ','.join(columnas1)

                    valores = []
                    fila =  list(concat_ensayos.iloc[i])
                    
                    for item in fila:
                            valores.append("'"+str(item)+"'")
                    
                    valores = ','.join(map(str, valores))

                    selectData = f'''SELECT correlativo,id_muestra,id_ensayo,stamp FROM copec_lims_prd.ensayo_rl
                                    WHERE correlativo = '{fila[0]}' and id_muestra = '{fila[3]}' 
                                    and id_ensayo = '{fila[4]}' and stamp = '{fila[11]}' '''

                    if queryRDS(selectData,'select') == None:
                        insertData = f''' insert into copec_lims_prd.ensayo_rl({(columnas)})
                                                                    values ({(valores)})'''
                        result = queryRDS(insertData, 'insert')

                    else:
                        i = 0
                        valores = []
                        for campos in columnas1:
                            valores.append(f''' {campos} = '{fila[i]}' ''')
                            i += 1

                        valores = ','.join(map(str, valores))

                        updateData = f''' update copec_lims_prd.ensayo_rl set {valores}
                                            WHERE correlativo = '{fila[0]}' and id_muestra = '{fila[3]}' 
                                            and id_ensayo = '{fila[4]}' and stamp = '{fila[11]}' '''

                        result = queryRDS(updateData, 'update')

                    print(result)
            else:
                print(insertData)           
        except Exception as e:
            print(e)


    ##########################################################################################
    # Termina con error
    ##########################################################################################
    if flag_error == 1:
        raise Exception("Lambda a finalizado con error.")













