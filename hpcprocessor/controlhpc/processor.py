# -*- coding: utf-8 -*-

from hpcprocessor.controlhpc.utils import work_data, load_uc_json
from hpcprocessor.controlhpc.loadconfig import get_use_uc_conversion




def get_groups_usages(data:dict, groups:set)->list:
    groups_usages = {}
    for group in groups:
        groups_usages[group] = get_group_usage(group,data[group])
    return groups_usages

def get_group_usage(group:str, data:list)->dict:
    """
    group,data[works] -> return {"time":hours, "uc":uc, "uch":uch}
    """
    group_jobduration = 0.0
    group_uc = 0.0
    group_uch = 0.0
    
    for work in data:
        work_d = work_data(work)
        duration = __get_jobduration(work_d)
        group_jobduration += duration

        uc = __get_uc(work_d)
        group_uc += uc
            
        hours = __s_h(duration)
        group_uch += hours * uc
    group_hours = __s_h(group_jobduration)
    return {"time":round(group_hours,2), "uc":round(group_uc,2), "uch":round(group_uch,2)}


user_jobduration = 0.0
user_uc = 0.0
user_uch = 0.0

def get_user_usage(user:str, data:list)->dict:
    """
    user,data[works] -> return {"time":hours, "uc":uc, "uch":uch}
    """
    user_jobduration = 0.0
    user_uc = 0.0
    user_uch = 0.0
    for work in data:
        work_d = work_data(work)
        if __get_owner(work_d) == user:
            duration = __get_jobduration(work_d)
            user_jobduration += duration

            uc = __get_uc(work_d)
            user_uc += uc

            hours = __s_h(duration)
            user_uch += hours * uc

    
    user_hours = __s_h(user_jobduration)
    return {"time":round(user_hours,2), "uc":round(user_uc,2), "uch":round(user_uch,2)}

def get_group_users_usage(group:str, owners:set, data:list)->list:
    usuarios = {}
    for owner in owners:
        user_result = get_user_usage(owner, data)
        if user_result['time'] != 0.0 and user_result['uc'] != 0.0: 
            usuarios[owner] = user_result

    return usuarios 






## priv

def __get_jobduration(work:list)->float:
    try:
        return work["JobDuration"]
    except:
        #print(work["CompletionDate"],"-", work["JobStartDate"],"---",work["CompletionDate"]-work["JobStartDate"])
        return float(work["RemoteWallClockTime"])


def __get_group(work:list)->str:
    try:
        return work["group"].upper()
    except:
        if work["UserLog"].split("/")[2] == "cephfs":
            return work["UserLog"].split("/")[4].upper()
        return work["UserLog"].split("/")[2].upper()

def __get_owner(work:list)->str:
    return work["Owner"]

def __get_status(work:list)->str:
    return work["Status"]

def __get_uc(work:list)->float:
    # si es más que 2 machacamos con el valor que viene
    uc = __uc_conversion(__get_startdname(work))
    if uc > 2:
        return uc
    cpus = __get_requestcpus(work)
    return cpus * uc


def __get_requestcpus(work:list)->int:
    return work["RequestCpus"]

def __get_startdname(work:list)->str:
    return work["StartdName"]

def __s_h(jobduration:float)->float:
    """
    seconds to hours
    """
    return jobduration/3600

def __uc_conversion(node:str)->float:
    conversion = load_uc_json()
    if get_use_uc_conversion() != 0:
        for key in conversion:
            if key in node:
                #print("1-",key," ",node," = ",conversion[key])
                return float(conversion[key])
    return 1.0

