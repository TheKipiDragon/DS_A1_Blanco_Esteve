import pywren_ibm_cloud as pywren
import json
import time

Bucket = 'cloud-object-storage-marcos-sistemas-distribuidos'
N_SLAVES = 5

def master(x, ibm_cos):
    write_permission_list = [] 
    object_list = []
    orden_list = {}

    ibm_cos.put_object(Bucket=Bucket, Key='results.json')
    time.sleep(x)
    result = ibm_cos.list_objects(Bucket=Bucket, Prefix='results.json')
    result2 = result.get('Contents')
    last_modified_old = result2[0].get('LastModified')

    cont = 0

    while(cont<N_SLAVES):
        # 1. monitor COS bucket each X seconds
        time.sleep(x)
        # 2. List all "p_write_{id}" files
        object_list = ibm_cos.list_objects_v2(Bucket=Bucket, Prefix='p_write_')
        # 3. Order objects by time of creation
        object_list2 = object_list.get('Contents')
        num_objects = len(object_list2)
        i = 0
        while (i<num_objects):
            data = {object_list2[i].get('LastModified') : object_list2[i].get('Key')}
            orden_list.update(data)
            i=i+1

        claves=orden_list.keys()

        # 4. Pop first object of the list "p_write_{id}"
        valor = orden_list.pop(next(iter(claves)))
        ident = valor[8:]
        # 5. Write empty "write_{id}" object into COS
        ibm_cos.put_object(Bucket=Bucket, Key='write_{'+ident+'}')

        # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
        write_permission_list.append(int(ident))
        ibm_cos.delete_object(Bucket=Bucket, Key='p_write_'+ident)

        # 7. Monitor "result.json" object each X seconds until it is updated
        result = ibm_cos.list_objects(Bucket=Bucket, Prefix='results.json')
        result2 = result.get('Contents')
        last_modified_act = result2[0].get('LastModified')
        while(last_modified_old==last_modified_act):
            time.sleep(x)
            result = ibm_cos.list_objects(Bucket=Bucket, Prefix='results.json')
            result2 = result.get('Contents')
            last_modified_act = result2[0].get('LastModified')

        last_modified_old=last_modified_act

        # 8. Delete from COS “write_{id}”
        ibm_cos.delete_object(Bucket=Bucket, Key='write_{'+ident+'}')

        cont+=1
        # 9. Back to step 1 until no "p_write_{id}" objects in the bucket

    return write_permission_list

def slave(id, x, ibm_cos): 
    
    ibm_cos.put_object(Bucket=Bucket, Key='p_write_'+str(id)) #subimos la peticion de escritura al COS
    finished=False
    
    while(not finished):
        time.sleep(x)
        num = ibm_cos.list_objects_v2(Bucket=Bucket, Prefix='write_{'+str(id)+'}')['KeyCount']
        if (num != 0):
            valor = {id : id}
            fichero = ibm_cos.get_object(Bucket=Bucket, Key='results.json')['Body'].read()
            data = {}
            if (bool(fichero)):
                data = json.loads(fichero)

            data.update(valor)

            ready = json.dumps(data)
            ibm_cos.put_object(Bucket=Bucket, Key='results.json', Body=ready)
            finished=True
            


if __name__ == '__main__':
    pw = pywren.ibm_cf_executor()
    start_time = time.time()
    pw.map(slave, range(N_SLAVES))
    pw.call_async(master, 0.2)
    write_permission_list = pw.get_result() 
    elapsed_time = time.time() - start_time
    print(write_permission_list)


    ibm_cos = pw.internal_storage.get_client()
    results = ibm_cos.get_object(Bucket=Bucket, Key='results.json')['Body'].read()
    data = json.loads(results)
    l=[]
    [l.extend([v]) for k,v in data.items()]
    print(l)

    if(write_permission_list==l): print("\nLas listas son iguales")
    else: print ("\nLas listas son diferentes")
    print ('Elapsed time: {0:.2f}'.format(elapsed_time))
