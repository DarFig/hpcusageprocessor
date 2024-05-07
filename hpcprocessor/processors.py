# -*- coding: utf-8 -*-

from hpcprocessor.controlhpc.controller import Controller
from hpcprocessor.controlhpc.processor import *
from hpcprocessor.sender import send


from datetime import date, timedelta,datetime




def day_process():
    # obtenemos las fechas
    yesterday = date.today() - timedelta(1)
    start_date = yesterday.strftime("%d/%m/%Y")
    end_date = date.today().strftime("%d/%m/%Y")

    # inicializamos el controller de elastic
    new_controller = Controller()
    
    # sacamos grupos 
    groups = new_controller.get_groups_names()
    
    __groups_process(new_controller, groups, start_date, end_date)
           

def manual_process():
    # Obtener el primer 
    first = datetime(2024, 4, 1)

    # Obtener el último día
    last = datetime(2024, 5, 7)

    # Iterar sobre cada día en mayo
    current_date  = first
    while current_date < last:
        # Llamar a la función day_process() con el formato especificado
        start_date = current_date.strftime("%d/%m/%Y")
        # Avanzar al siguiente día
        current_date += timedelta(days=1)
        end_date = current_date.strftime("%d/%m/%Y")
        
        # inicializamos el controller de elastic
        new_controller = Controller()
        
        # sacamos grupos 
        groups = new_controller.get_groups_names()
        
        __groups_process(new_controller, groups, start_date, end_date)
        
        
        



    
    
def __groups_process(new_controller, groups, start_date, end_date):
    for group in groups:
        if group.isupper():
            #datos de trabajos para el rango de tiempo y el grupo
            data = new_controller.match_date_range(start_date, end_date,group)
            # posibles owners de los trabajos
            owners = new_controller.get_group_users(group)
            
            results = {}
            # obtenemos los datos agregados del grupo
            results = get_group_usage(group, data)
            if results['time'] != 0.0 and results['uc'] != 0.0:
                results['group'] = group
                results['date'] = start_date
                
                # enviamos
                index_name = 'day_hpc_group_data'
                send(results,index_name)
                
                # obtenemos los datos por usuario del grupo
                users = get_group_users_usage(group, owners, data)
                
                users['group'] = group
                users['date'] = start_date
                
                #enviamos
                index_name = 'day_hpc_users_data'
                send(users,index_name)
        
        

    